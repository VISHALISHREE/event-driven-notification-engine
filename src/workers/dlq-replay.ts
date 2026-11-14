import "dotenv/config";
import {
  connectRabbitMQ,
  disconnectRabbitMQ,
  notificationQueue,
  deadLetterQueue,
} from "../infra/rabbitmq/rabbitmq.client.js";

interface DeliveryMessage {
  notificationId: string;
  createdAt: string;
  channel: string;
  subject: string | null;
  body: string | null;
}

function isValidMessage(content: Buffer): boolean {
  try {
    const value = JSON.parse(content.toString()) as DeliveryMessage;

    if (
      !value.notificationId ||
      !value.createdAt ||
      !["EMAIL", "IN_APP"].includes(value.channel) ||
      Number.isNaN(new Date(value.createdAt).getTime())
    ) {
      return false;
    }

    return true;
  } catch {
    return false;
  }
}

async function inspectDLQ(): Promise<void> {
  const channel = await connectRabbitMQ();

  try {
    const message = await channel.get(deadLetterQueue, {
      noAck: false,
    });

    if (!message) {
      console.log("DLQ is empty.");
      return;
    }

    console.log("Message found in DLQ:");
    console.log("Content:", message.content.toString());
    console.log("Headers:", message.properties.headers);
    console.log("Message ID:", message.properties.messageId);
    console.log("Redelivered:", message.fields.redelivered);

    // Put the message back. Inspection must not remove it.
    channel.nack(message, false, true);
    console.log("Message safely returned to DLQ.");
  } finally {
    await disconnectRabbitMQ();
  }
}

async function replayDLQ(): Promise<void> {
  const channel = await connectRabbitMQ();

  try {
    let replayed = 0;

    for (let i = 0; i < 10; i++) {
      const message = await channel.get(deadLetterQueue, {
        noAck: false,
      });

      if (!message) {
        console.log("No more messages in DLQ.");
        break;
      }

      if (!isValidMessage(message.content)) {
        console.warn(
          "Invalid message left in DLQ:",
          message.content.toString(),
        );

        channel.nack(message, false, true);
        break;
      }

      channel.sendToQueue(
        notificationQueue,
        message.content,
        {
          persistent: true,
          contentType: "application/json",
          messageId: message.properties.messageId,
          headers: {
            ...message.properties.headers,
            replayedFromDLQ: true,
            replayedAt: new Date().toISOString(),
          },
        },
      );

      // Confirm publication before acknowledging the original.
      await channel.waitForConfirms();
      channel.ack(message);

      replayed++;
      console.log(`Successfully replayed message ${replayed}`);
    }

    console.log(`DLQ replay completed. Replayed: ${replayed}`);
  } finally {
    await disconnectRabbitMQ();
  }
}

async function main(): Promise<void> {
  const operation = process.argv[2];

  if (operation === "inspect") {
    await inspectDLQ();
  } else if (operation === "replay") {
    await replayDLQ();
  } else {
    console.log("Usage:");
    console.log("npm run worker:dlq:inspect");
    console.log("npm run worker:dlq:replay");
  }
}

main().catch(async (error) => {
  console.error("DLQ utility failed:", error);
  await disconnectRabbitMQ();
  process.exitCode = 1;
});
