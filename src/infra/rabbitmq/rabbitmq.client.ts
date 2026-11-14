import "dotenv/config";
import * as amqp from "amqplib";
import type { ChannelModel, ConfirmChannel } from "amqplib";

export const notificationQueue =
  process.env.RABBITMQ_NOTIFICATION_QUEUE ??
  "notification.delivery.v2";

export const deadLetterExchange = "notification.dlx";
export const deadLetterQueue = "notification.delivery.dlq";
export const deadLetterRoutingKey = "notification.delivery.dead";

let connection: ChannelModel | null = null;
let channel: ConfirmChannel | null = null;

export async function connectRabbitMQ(): Promise<ConfirmChannel> {
  if (connection && channel) return channel;

  const url =
    process.env.RABBITMQ_URL ??
    "amqp://127.0.0.1:5672";

  connection = await amqp.connect(url);

  connection.on("error", (error) => {
    console.error("RabbitMQ connection error:", error);
  });

  connection.on("close", () => {
    console.warn("RabbitMQ connection closed");
    connection = null;
    channel = null;
  });

  channel = await connection.createConfirmChannel();

  channel.on("error", (error) => {
    console.error("RabbitMQ channel error:", error);
  });

  // Dead-letter exchange
  await channel.assertExchange(deadLetterExchange, "direct", {
    durable: true,
  });

  // Dead-letter queue
  await channel.assertQueue(deadLetterQueue, {
    durable: true,
  });

  // Connect DLQ to exchange
  await channel.bindQueue(
    deadLetterQueue,
    deadLetterExchange,
    deadLetterRoutingKey,
  );

  // Main delivery queue
  await channel.assertQueue(notificationQueue, {
    durable: true,
    arguments: {
      "x-dead-letter-exchange": deadLetterExchange,
      "x-dead-letter-routing-key": deadLetterRoutingKey,
    },
  });

  console.log("RabbitMQ connected.");
  console.log(`Main queue: ${notificationQueue}`);
  console.log(`Dead-letter queue: ${deadLetterQueue}`);

  return channel;
}

export async function disconnectRabbitMQ(): Promise<void> {
  const currentChannel = channel;
  const currentConnection = connection;

  channel = null;
  connection = null;

  if (currentChannel) {
    await currentChannel.close().catch(() => undefined);
  }

  if (currentConnection) {
    await currentConnection.close().catch(() => undefined);
  }

  console.log("RabbitMQ disconnected");
}