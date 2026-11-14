import { createHash } from "node:crypto";
import { Prisma } from "../../generated/prisma/client.js";
import { prisma } from "../../infra/database/prisma.js";
import { redis, connectRedis } from "../../infra/redis/redis.client.js";

const IDEMPOTENCY_TTL_HOURS = 24;

interface IdempotencyResult {
  body: Prisma.InputJsonValue;
  statusCode: number;
}

interface IdempotencyOptions {
  scope: string;
  key: string;
  requestBody: unknown;
  execute: (
    tx: Prisma.TransactionClient
  ) => Promise<IdempotencyResult>;
}

interface CachedResponse {
  requestHash: string;
  body: Prisma.InputJsonValue;
  statusCode: number;
  expiresAt: string;
}

export class IdempotencyConflictError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "IdempotencyConflictError";
  }
}

// Generate a consistent string regardless of object key order.
function stableStringify(value: unknown): string {
  if (value === null || typeof value !== "object") {
    const result = JSON.stringify(value);
    return result === undefined ? String(value) : result;
  }

  if (Array.isArray(value)) {
    return "[" + value.map(stableStringify).join(",") + "]";
  }

  const object = value as Record<string, unknown>;

  const sortedEntries = Object.keys(object)
    .sort()
    .map((key) => {
      return (
        JSON.stringify(key) +
        ":" +
        stableStringify(object[key])
      );
    });

  return "{" + sortedEntries.join(",") + "}";
}

// Generate SHA-256 request hash.
function createRequestHash(body: unknown): string {
  return createHash("sha256")
    .update(stableStringify(body))
    .digest("hex");
}

// Generate a Redis cache key.
function createCacheKey(scope: string, key: string): string {
  const hash = createHash("sha256")
    .update(scope + ":" + key)
    .digest("hex");

  return "idempotency:v1:" + hash;
}

// Validate cached response before using it.
function isValidCachedResponse(
  value: unknown
): value is CachedResponse {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return false;
  }

  const response = value as Record<string, unknown>;

  return (
    typeof response.requestHash === "string" &&
    response.body !== null &&
    typeof response.body === "object" &&
    !Array.isArray(response.body) &&
    Number.isInteger(response.statusCode) &&
    typeof response.expiresAt === "string" &&
    !Number.isNaN(Date.parse(response.expiresAt))
  );
}

// Read a completed response from Redis.
async function readCachedResponse(
  cacheKey: string
): Promise<CachedResponse | null> {
  try {
    await connectRedis();

    const cached = await redis.get(cacheKey);

    if (!cached) {
      return null;
    }

    const parsed: unknown = JSON.parse(cached);

    if (!isValidCachedResponse(parsed)) {
      console.warn("Invalid Redis cache entry.");
      return null;
    }

    if (Date.parse(parsed.expiresAt) <= Date.now()) {
      return null;
    }

    return parsed;
  } catch (error) {
    console.warn(
      "Redis read failed. Falling back to PostgreSQL.",
      error instanceof Error ? error.message : error
    );

    return null;
  }
}

// Store completed response in Redis.
async function cacheResponse(
  cacheKey: string,
  response: CachedResponse
): Promise<void> {
  try {
    await connectRedis();

    const ttlSeconds = Math.floor(
      (Date.parse(response.expiresAt) - Date.now()) / 1000
    );

    if (ttlSeconds <= 0) {
      return;
    }

    await redis.set(
      cacheKey,
      JSON.stringify(response),
      "EX",
      ttlSeconds
    );
  } catch (error) {
    console.warn(
      "Redis write failed. PostgreSQL remains authoritative.",
      error instanceof Error ? error.message : error
    );
  }
}

// Main idempotency service.
export async function executeIdempotently(
  options: IdempotencyOptions
): Promise<{
  body: Prisma.InputJsonValue;
  statusCode: number;
  replayed: boolean;
}> {
  const { scope, key, requestBody, execute } = options;

  const requestHash = createRequestHash(requestBody);
  const cacheKey = createCacheKey(scope, key);

  // STEP 1: Check Redis first.
  const cached = await readCachedResponse(cacheKey);

  if (cached) {
    if (cached.requestHash !== requestHash) {
      throw new IdempotencyConflictError(
        "This idempotency key was already used with a different request."
      );
    }

    return {
      body: cached.body,
      statusCode: cached.statusCode,
      replayed: true,
    };
  }

  // STEP 2: Redis miss. Check PostgreSQL.
  const result = await prisma.$transaction(async (tx) => {

    // Prevent concurrent requests using the same key.
    // Parameters are safely bound; no SQL string interpolation.
    await tx.$queryRawUnsafe<{ locked: number }[]>(
      "WITH lock AS MATERIALIZED (" +
        "SELECT pg_advisory_xact_lock(" +
        "hashtext($1), hashtext($2)" +
        ")" +
        ") " +
        "SELECT 1::int AS locked FROM lock",
      scope,
      key
    );

    const existing = await tx.idempotencyKey.findUnique({
      where: {
        scope_key: {
          scope,
          key,
        },
      },
    });

    const now = new Date();

    // Existing unexpired key.
    if (existing && existing.expiresAt > now) {

      if (existing.requestHash !== requestHash) {
        throw new IdempotencyConflictError(
          "This idempotency key was already used with a different request."
        );
      }

      if (
        existing.status === "COMPLETED" &&
        existing.responseBody !== null &&
        existing.statusCode !== null
      ) {
        return {
          body: existing.responseBody as Prisma.InputJsonValue,
          statusCode: existing.statusCode,
          replayed: true,
          expiresAt: existing.expiresAt,
        };
      }

      throw new IdempotencyConflictError(
        "A request with this idempotency key is already in progress or has failed."
      );
    }

    // Delete expired record before reusing its key.
    if (existing) {
      await tx.idempotencyKey.delete({
        where: {
          scope_key: {
            scope,
            key,
          },
        },
      });
    }

    const expiresAt = new Date(
      now.getTime() +
        IDEMPOTENCY_TTL_HOURS * 60 * 60 * 1000
    );

    // Create idempotency record.
    await tx.idempotencyKey.create({
      data: {
        scope,
        key,
        requestHash,
        status: "IN_PROGRESS",
        expiresAt,
      },
    });

    // Execute business operation in the same transaction.
    const response = await execute(tx);

    // Mark idempotency record as completed.
    await tx.idempotencyKey.update({
      where: {
        scope_key: {
          scope,
          key,
        },
      },
      data: {
        status: "COMPLETED",
        responseBody: response.body,
        statusCode: response.statusCode,
      },
    });

    return {
      body: response.body,
      statusCode: response.statusCode,
      replayed: false,
      expiresAt,
    };
  });

  // STEP 3: Cache only after PostgreSQL transaction commits.
  await cacheResponse(cacheKey, {
    requestHash,
    body: result.body,
    statusCode: result.statusCode,
    expiresAt: result.expiresAt.toISOString(),
  });

  return {
    body: result.body,
    statusCode: result.statusCode,
    replayed: result.replayed,
  };
}