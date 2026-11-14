import "dotenv/config";
import { randomUUID } from "node:crypto";
import { prisma } from "../infra/database/prisma.js";
import {
  connectKafka,
  disconnectKafka,
  kafkaProducer,
} from "../infra/kafka/kafka.producer.js";

const BATCH_SIZE = 20;
const MAX_ATTEMPTS = 12;
const LEASE_SECONDS = 120;
const POLL_INTERVAL_MS = 1000;

const workerId = `outbox-worker-${randomUUID()}`;

interface ClaimedMessage {
  id: string;
  topic: string;
  aggregateId: string;
  payload: unknown;
  attempts: number;
  leaseToken: string;
}

let stopping = false;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function claimMessages(): Promise<ClaimedMessage[]> {
  const leaseToken = randomUUID();

  return prisma.$transaction(async (tx) => {
    return tx.$queryRaw<ClaimedMessage[]>`
      WITH candidates AS (
        SELECT "id"
        FROM "OutboxMessage"
        WHERE
          (
            "status" IN ('PENDING', 'FAILED')
            AND "availableAt" <= NOW()
            AND "attempts" < ${MAX_ATTEMPTS}
          )
          OR
(
  "status" = 'PROCESSING'
  AND "lockedUntil" <= NOW()
  AND "attempts" < ${MAX_ATTEMPTS}
)
        ORDER BY "createdAt" ASC
        LIMIT ${BATCH_SIZE}
        FOR UPDATE SKIP LOCKED
      )
      UPDATE "OutboxMessage" AS outbox
      SET
        "status" = 'PROCESSING',
        "attempts" = outbox."attempts" + 1,
        "lockedUntil" = NOW() + (${LEASE_SECONDS} * INTERVAL '1 second'),
        "leaseToken" = ${leaseToken}::uuid,
        "leaseOwner" = ${workerId},
        "lastAttemptAt" = NOW()
      FROM candidates
      WHERE outbox."id" = candidates."id"
      RETURNING
        outbox."id",
        outbox."topic",
        outbox."aggregateId",
        outbox."payload",
        outbox."attempts",
        outbox."leaseToken"
    `;
  });
}

function retryDelay(attempt: number): number {
  const baseDelay = Math.min(1000 * 2 ** (attempt - 1), 15 * 60 * 1000);
  const jitter = Math.random() * 1000;

  return baseDelay + jitter;
}

async function markPublished(message: ClaimedMessage): Promise<void> {
  const result = await prisma.outboxMessage.updateMany({
    where: {
      id: message.id,
      status: "PROCESSING",
      leaseOwner: workerId,
      leaseToken: message.leaseToken,
    },
    data: {
      status: "PUBLISHED",
      publishedAt: new Date(),
      lockedUntil: null,
      leaseToken: null,
      leaseOwner: null,
      lastError: null,
    },
  });

  if (result.count === 0) {
    console.warn(`Lease lost before marking message ${message.id} published`);
  } else {
    console.log(`Published outbox message: ${message.id}`);
  }
}

async function markFailed(
  message: ClaimedMessage,
  error: unknown,
): Promise<void> {
  const errorMessage =
    error instanceof Error ? error.message : String(error);

  const exhausted = message.attempts >= MAX_ATTEMPTS;

  const result = await prisma.outboxMessage.updateMany({
    where: {
      id: message.id,
      status: "PROCESSING",
      leaseOwner: workerId,
    },
    data: {
      status: "FAILED",
      availableAt: exhausted
        ? new Date("9999-12-31T23:59:59.999Z")
        : new Date(Date.now() + retryDelay(message.attempts)),
      lockedUntil: null,
      leaseToken: null,
      leaseOwner: null,
      lastError: errorMessage.slice(0, 2000),
    },
  });

  if (result.count === 0) {
    console.warn(`Lease lost while handling message ${message.id}`);
    return;
  }

  if (exhausted) {
    console.error(
      `Message ${message.id} exhausted its ${MAX_ATTEMPTS} attempts`,
    );
  } else {
    console.error(
      `Message ${message.id} failed. Attempt ${message.attempts}/${MAX_ATTEMPTS}`,
      errorMessage,
    );
  }
}

async function publishMessage(message: ClaimedMessage): Promise<void> {
  try {
    await kafkaProducer.send({
      topic: message.topic,
      acks: -1,
      messages: [
        {
          key: message.aggregateId,
          value: JSON.stringify(message.payload),
          headers: {
            outboxMessageId: message.id,
          },
        },
      ],
    });
  } catch (error) {
    await markFailed(message, error);
    return;
  }

  // Keep this separate from the Kafka send:
  // if the database update fails after Kafka accepts the message,
  // the lease will expire and the message may be published again.
  await markPublished(message);
}

async function processBatch(): Promise<number> {
  const messages = await claimMessages();

  for (const message of messages) {
    await publishMessage(message);
  }

  return messages.length;
}

async function start(): Promise<void> {
  console.log(`Starting Outbox Relay: ${workerId}`);

  await prisma.$connect();
  await connectKafka();

  console.log("Outbox Relay connected to PostgreSQL and Kafka");

  while (!stopping) {
    try {
      const processed = await processBatch();

      if (processed === 0) {
        await sleep(POLL_INTERVAL_MS);
      }
    } catch (error) {
      console.error("Outbox Relay batch error:", error);
      await sleep(POLL_INTERVAL_MS);
    }
  }
}

process.once("SIGINT", () => {
  stopping = true;
});

process.once("SIGTERM", () => {
  stopping = true;
});

start()
  .catch((error) => {
    console.error("Outbox Relay stopped unexpectedly:", error);
    process.exitCode = 1;
  })
  .finally(async () => {
    await disconnectKafka();
    await prisma.$disconnect();
    console.log("Outbox Relay shut down");
  });