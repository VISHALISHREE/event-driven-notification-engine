import "dotenv/config";
import type { ConsumeMessage, ConfirmChannel } from "amqplib";
import {
  connectRabbitMQ,
  disconnectRabbitMQ,
  notificationQueue,
} from "../infra/rabbitmq/rabbitmq.client.js";
import { prisma } from "../infra/database/prisma.js";

interface DeliveryMessage {
  notificationId: string;
  createdAt: string;
  channel: string;
  subject: string | null;
  body: string | null;
}

let rabbitChannel: ConfirmChannel | null = null;
let consumerTag: string | null = null;
let shuttingDown = false;

function parseMessage(content: Buffer): DeliveryMessage {
  const value = JSON.parse(content.toString()) as DeliveryMessage;

  if (
    !value.notificationId ||
    !value.createdAt ||
    !["EMAIL", "SMS", "IN_APP", "PUSH", "WHATSAPP"].includes(value.channel)
  ) {
    throw new Error("Invalid delivery message");
  }

  return value;
}

async function getMockProvider(
  channel: "EMAIL" | "SMS" | "IN_APP" | "PUSH" | "WHATSAPP"
) {
  const name = `${channel.toLowerCase()}-mock`;

  return prisma.provider.upsert({
    where: {
      name_channel: { name, channel },
    },
    create: {
      name,
      channel,
      status: "ACTIVE",
      priority: 1,
      supportsIdempotency: true,
    },
update: {},
  });
}

async function processDelivery(message: DeliveryMessage): Promise<void> {
  const createdAt = new Date(message.createdAt);

  const notification = await prisma.notification.findFirst({
    where: {
      id: message.notificationId,
      createdAt,
    },
  });

  if (!notification) {
    console.warn("Notification not found:", message.notificationId);
    return;
  }

  // Atomically claim the notification. Duplicate messages cannot
  // process a notification that another worker has already claimed.
const claim = await prisma.notification.updateMany({
  where: {
    id: notification.id,
    createdAt: notification.createdAt,
    status: {
      in: ["PENDING", "QUEUED", "RETRY_SCHEDULED"],
    },
  },
  data: {
    status: "PROCESSING",
  },
});

  if (claim.count !== 1) {
    console.log(
      `Skipping duplicate or already processed notification: ${notification.id}`,
    );
    return;
  }

  const channel = notification.channel as
  | "EMAIL"
  | "SMS"
  | "IN_APP"
  | "PUSH"
  | "WHATSAPP";
  const provider = await getMockProvider(channel);

 if (provider.status === "DOWN") {
  const now = new Date();
  const cooldownMs = 5 * 60_000;

  const latestHealth = await prisma.providerHealth.findFirst({
    where: { providerId: provider.id },
    orderBy: { checkedAt: "desc" },
  });

  const cooldownEndsAt = latestHealth
    ? new Date(latestHealth.checkedAt.getTime() + cooldownMs)
    : new Date(now.getTime() + cooldownMs);

  if (cooldownEndsAt > now) {
    await prisma.notification.updateMany({
      where: {
        id: notification.id,
        createdAt: notification.createdAt,
        status: "PROCESSING",
      },
      data: {
        status: "RETRY_SCHEDULED",
        nextRetryAt: cooldownEndsAt,
        lastError: "Provider circuit is open. Waiting for cooldown.",
      },
    });

    return;
  }

  const probeLock = await prisma.provider.updateMany({
    where: {
      id: provider.id,
      status: "DOWN",
      OR: [
        { probeLockedUntil: null },
        { probeLockedUntil: { lte: now } },
      ],
    },
    data: {
      probeLockedUntil: new Date(now.getTime() + cooldownMs),
    },
  });

  if (probeLock.count !== 1) {
    await prisma.notification.updateMany({
      where: {
        id: notification.id,
        createdAt: notification.createdAt,
        status: "PROCESSING",
      },
      data: {
        status: "RETRY_SCHEDULED",
        nextRetryAt: new Date(now.getTime() + cooldownMs),
        lastError: "Another worker is testing provider recovery.",
      },
    });

    return;
  }
}
  const attemptNumber = notification.retryCount + 1;
  const startedAt = Date.now();

  const attempt = await prisma.deliveryAttempt.create({
    data: {
      notificationId: notification.id,
      notificationCreatedAt: notification.createdAt,
      providerId: provider.id,
      attemptNumber,
      status: "STARTED",
      requestMetadata: {
        channel,
        mock: true,
      },
    },
  });

  try {
    // Mock provider failure can be tested by setting:
    // MOCK_PROVIDER_FAIL_CHANNELS=EMAIL
    const failingChannels = (
      process.env.MOCK_PROVIDER_FAIL_CHANNELS ?? ""
    )
      .split(",")
      .map((item) => item.trim().toUpperCase())
      .filter(Boolean);

    if (failingChannels.includes(channel)) {
      throw new Error(`Simulated ${channel} provider failure`);
    }

    if (!message.body?.trim()) {
      throw new Error("Notification body is empty");
    }

    // No real email or external message is sent.
    const providerMessageId = `mock-${attempt.id}`;

    console.log(
      `[MOCK ${channel}] Accepted notification ${notification.id}`,
    );

    const previousSuccessHealth = await prisma.providerHealth.findFirst({
  where: { providerId: provider.id },
  orderBy: { checkedAt: "desc" },
});

const successTotalRequests =
  (previousSuccessHealth?.totalRequests ?? 0) + 1;

const successFailedRequests =
  previousSuccessHealth?.failedRequests ?? 0;

    await prisma.$transaction([
      prisma.deliveryAttempt.update({
        where: { id: attempt.id },
        data: {
          status: "ACCEPTED",
          providerMessageId,
          responseMetadata: {
            accepted: true,
            mock: true,
          },
          completedAt: new Date(),
          latencyMs: Date.now() - startedAt,
        },
      }),

      prisma.notification.updateMany({
        where: {
          id: notification.id,
          createdAt: notification.createdAt,
          status: "PROCESSING",
        },
        data: {
          status: "SENT",
          sentAt: new Date(),
          lastError: null,
        },
      }),

            prisma.providerHealth.create({
        data: {
          providerId: provider.id,
          status: "ACTIVE",
          successRate:
            (successTotalRequests - successFailedRequests) /
            successTotalRequests,
          avgLatencyMs: Date.now() - startedAt,
          totalRequests: successTotalRequests,
          failedRequests: successFailedRequests,
        },
      }),

      prisma.provider.update({
        where: { id: provider.id },
        data: {
          status: "ACTIVE",
          probeLockedUntil: null,
        },
      }),
    ]);

    console.log(`Notification ${notification.id}: SENT`);
  } catch (error) {
    const errorMessage =
      error instanceof Error ? error.message : "Unknown delivery error";

const recentHealth = await prisma.providerHealth.findMany({
  where: { providerId: provider.id },
  orderBy: { checkedAt: "desc" },
  take: 2,
});

const circuitShouldOpen =
  recentHealth.length === 2 &&
  recentHealth.every(
    (record) => record.status === "DEGRADED" || record.status === "DOWN",
  );

const previousHealth = recentHealth[0];
const totalRequests = (previousHealth?.totalRequests ?? 0) + 1;
const failedRequests = (previousHealth?.failedRequests ?? 0) + 1;

const providerStatus = circuitShouldOpen ? "DOWN" : "DEGRADED";

await prisma.$transaction([
  prisma.providerHealth.create({
    data: {
      providerId: provider.id,
      status: providerStatus,
      successRate: (totalRequests - failedRequests) / totalRequests,
      avgLatencyMs: Date.now() - startedAt,
      totalRequests,
      failedRequests,
    },
  }),
prisma.provider.update({
  where: { id: provider.id },
  data: {
    status: providerStatus,
    probeLockedUntil:
      providerStatus === "DOWN"
        ? new Date(Date.now() + 5 * 60_000)
        : null,
  },
}),
]);

    const nextRetryCount = notification.retryCount + 1;
    const exhausted = nextRetryCount >= notification.maxRetries;

    const delayMs = Math.min(
      60_000 * 2 ** (attemptNumber - 1),
      15 * 60_000,
    );

    const nextRetryAt = exhausted
      ? null
      : new Date(Date.now() + delayMs);

    await prisma.$transaction([
      prisma.deliveryAttempt.update({
        where: { id: attempt.id },
        data: {
          status: "FAILED",
          errorCode: "MOCK_PROVIDER_ERROR",
          errorMessage,
          completedAt: new Date(),
          latencyMs: Date.now() - startedAt,
        },
      }),

      prisma.notification.updateMany({
        where: {
          id: notification.id,
          createdAt: notification.createdAt,
          status: "PROCESSING",
        },
        data: {
          retryCount: nextRetryCount,
          status: exhausted ? "DEAD_LETTER" : "RETRY_SCHEDULED",
          nextRetryAt,
          lastError: errorMessage,
        },
      }),
    ]);

    console.error(
      `Notification ${notification.id}: ${
        exhausted ? "DEAD_LETTER" : "RETRY_SCHEDULED"
      }`,
      errorMessage,
    );
  }
}

async function handleMessage(message: ConsumeMessage): Promise<void> {
  if (!rabbitChannel) return;

  let delivery: DeliveryMessage;

  // Invalid messages go directly to RabbitMQ DLQ.
  try {
    delivery = parseMessage(message.content);

    const createdAt = new Date(delivery.createdAt);

    if (Number.isNaN(createdAt.getTime())) {
      throw new Error("Invalid notification createdAt date");
    }
  } catch (error) {
    console.error("Invalid RabbitMQ message:", error);

    rabbitChannel.nack(message, false, false);
    return;
  }

  // Infrastructure/database errors are requeued.
  try {
    await processDelivery(delivery);
    rabbitChannel.ack(message);
  } catch (error) {
    console.error("Delivery processing failed:", error);

    rabbitChannel.nack(message, false, true);
  }
}
async function shutdown(): Promise<void> {
  if (shuttingDown) return;
  shuttingDown = true;

  console.log("Stopping delivery worker...");

  try {
    if (rabbitChannel && consumerTag) {
      await rabbitChannel.cancel(consumerTag);
    }
  } catch (error) {
    console.error("Consumer cancellation error:", error);
  }

  await disconnectRabbitMQ();
  await prisma.$disconnect();

  console.log("Delivery worker stopped");
}

async function main(): Promise<void> {
  rabbitChannel = await connectRabbitMQ();

  await rabbitChannel.prefetch(10);

  const result = await rabbitChannel.consume(
    notificationQueue,
    (message) => {
      if (message) {
        void handleMessage(message);
      }
    },
    { noAck: false },
  );

  consumerTag = result.consumerTag;

  console.log(
    `Delivery worker listening on queue: ${notificationQueue}`,
  );
}

process.on("SIGINT", () => void shutdown());
process.on("SIGTERM", () => void shutdown());

main().catch(async (error) => {
  console.error("Delivery worker startup failed:", error);
  await shutdown();
  process.exitCode = 1;
});
