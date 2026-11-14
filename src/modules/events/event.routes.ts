import { FastifyPluginAsync } from "fastify";
import { Prisma } from "../../generated/prisma/client.js";
import {
  EventType,
  EventProcessingStatus,
  OutboxStatus,
} from "../../generated/prisma/enums.js";
import { prisma } from "../../infra/database/prisma.js";
import { eventsTopic } from "../../infra/kafka/kafka.producer.js";
import {
  executeIdempotently,
  IdempotencyConflictError,
} from "../../common/idempotency/idempotency.service.js";

const eventRoutes: FastifyPluginAsync = async (app) => {
  app.post("/", async (request, reply) => {
    const body = request.body as {
      producer?: string;
      externalEventId?: string;
      eventType?: string;
      occurredAt?: string;
      payload?: unknown;
    };

    if (
      !body?.producer ||
      !body.externalEventId ||
      !body.eventType ||
      !body.occurredAt ||
      body.payload === undefined
    ) {
      return reply.code(400).send({
        message: "Missing required event fields",
      });
    }

    if (!Object.values(EventType).includes(body.eventType as EventType)) {
      return reply.code(400).send({
        message: "Invalid eventType",
      });
    }

    const occurredAt = new Date(body.occurredAt);

    if (Number.isNaN(occurredAt.getTime())) {
      return reply.code(400).send({
        message: "Invalid occurredAt date",
      });
    }

    const createEvent = async (
      tx: Prisma.TransactionClient
    ): Promise<{
      body: Prisma.InputJsonValue;
      statusCode: number;
    }> => {
      const existing = await tx.event.findUnique({
        where: {
          producer_externalEventId: {
            producer: body.producer!,
            externalEventId: body.externalEventId!,
          },
        },
      });

      if (existing) {
        return {
          body: {
            message: "Duplicate event ignored",
            duplicate: true,
            eventId: existing.id,
          },
          statusCode: 200,
        };
      }

      const event = await tx.event.create({
        data: {
          producer: body.producer!,
          externalEventId: body.externalEventId!,
          eventType: body.eventType as EventType,
          occurredAt,
          payload: body.payload as Prisma.InputJsonValue,
          status: EventProcessingStatus.RECEIVED,
        },
      });

      await tx.outboxMessage.create({
        data: {
          eventId: event.id,
          aggregateType: "Event",
          aggregateId: event.id,
          topic: eventsTopic,
          payload: {
            eventId: event.id,
            producer: event.producer,
            externalEventId: event.externalEventId,
            eventType: event.eventType,
            schemaVersion: event.schemaVersion,
            occurredAt: event.occurredAt.toISOString(),
            payload: body.payload as Prisma.InputJsonValue,
          },
          status: OutboxStatus.PENDING,
        },
      });

      return {
        body: {
          message: "Event accepted",
          eventId: event.id,
          status: event.status,
        },
        statusCode: 201,
      };
    };

    try {
      const idempotencyKey = request.headers["idempotency-key"];

      // Preserve existing behaviour when no key is supplied.
      if (!idempotencyKey) {
        const result = await prisma.$transaction(createEvent);

        return reply.code(result.statusCode).send(result.body);
      }

      if (
        typeof idempotencyKey !== "string" ||
        idempotencyKey.length > 200
      ) {
        return reply.code(400).send({
          message: "Invalid Idempotency-Key header",
        });
      }

      const result = await executeIdempotently({
        scope: "POST:/api/v1/events",
        key: idempotencyKey,
        requestBody: body,
        execute: createEvent,
      });

      return reply
        .header("Idempotency-Replayed", String(result.replayed))
        .code(result.statusCode)
        .send(result.body);
    } catch (error) {
      if (error instanceof IdempotencyConflictError) {
        return reply.code(409).send({
          message: error.message,
        });
      }

      console.error("IDEMPOTENCY API ERROR:", error);
      request.log.error(error);

      return reply.code(500).send({
        message: "Failed to accept event",
      });
    }
  });
};

export default eventRoutes;