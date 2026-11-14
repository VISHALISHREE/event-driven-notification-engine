import "dotenv/config";
import {
  connectRabbitMQ,
  disconnectRabbitMQ,
  notificationQueue,
} from "../infra/rabbitmq/rabbitmq.client.js";
import { prisma } from "../infra/database/prisma.js";

const BATCH_SIZE = 20;
const POLL_INTERVAL_MS = 2000;

let running = true;

interface DeliveryMessage {
  notificationId: string;
  createdAt: string;
  channel: string;
  subject: string | null;
  body: string | null;
}

async function publishMessage(
  rabbitChannel: Awaited<ReturnType<typeof connectRabbitMQ>>,
  message: DeliveryMessage,
): Promise<void> {
  const content = Buffer.from(JSON.stringify(message));

  await new Promise<void>((resolve, reject) => {
    rabbitChannel.sendToQueue(
      notificationQueue,
      content,
      {
        persistent: true,
        contentType: "application/json",
        messageId: `${message.notificationId}:${message.createdAt}`,
        timestamp: Math.floor(Date.now() / 1000),
      },
      (error) => {
        if (error) {
          reject(error);
        } else {
          resolve();
        }
      },
    );
  });
}

async function dispatchBatch(): Promise<number> {
  const now = new Date();

  const notifications = await prisma.notification.findMany({
    where: {
      OR: [
        {
          status: "PENDING",
          OR: [
            { scheduledAt: null },
            { scheduledAt: { lte: now } },
          ],
        },
        {
          status: "RETRY_SCHEDULED",
          nextRetryAt: { lte: now },
        },
      ],
    },
    orderBy: [
      { priority: "asc" },
      { createdAt: "asc" },
    ],
    take: BATCH_SIZE,
  });

  const rabbitChannel = await connectRabbitMQ();
  let published = 0;

  for (const notification of notifications) {
    const message: DeliveryMessage = {
      notificationId: notification.id,
      createdAt: notification.createdAt.toISOString(),
      channel: notification.channel,
      subject: notification.renderedSubject,
      body: notification.renderedBody,
    };

    try {
      await publishMessage(rabbitChannel, message);

      const result = await prisma.notification.updateMany({
        where: {
          id: notification.id,
          createdAt: notification.createdAt,
          status: {
            in: ["PENDING", "RETRY_SCHEDULED"],
          },
        },
        data: {
          status: "QUEUED",
          nextRetryAt: null,
        },
      });

      if (result.count === 1) {
        published++;
      }
    } catch (error) {
      console.error(
        `Failed to dispatch notification ${notification.id}:`,
        error,
      );
    }
  }

  return published;
}

async function main(): Promise<void> {
  console.log("Notification dispatcher starting...");

  try {
    await connectRabbitMQ();

    while (running) {
      try {
        const count = await dispatchBatch();

        if (count > 0) {
          console.log(`Dispatched ${count} notification(s)`);
        }
      } catch (error) {
        console.error("Dispatcher batch failed:", error);
      }

      if (running) {
        await new Promise((resolve) =>
          setTimeout(resolve, POLL_INTERVAL_MS),
        );
      }
    }
  } finally {
    await disconnectRabbitMQ();
    await prisma.$disconnect();
    console.log("Notification dispatcher stopped");
  }
}

process.on("SIGINT", () => {
  running = false;
});

process.on("SIGTERM", () => {
  running = false;
});

main().catch((error) => {
  console.error("Dispatcher startup failed:", error);
  process.exitCode = 1;
});
