import "dotenv/config";
import { Kafka, Producer } from "kafkajs";

const brokers = (process.env.KAFKA_BROKERS ?? "localhost:9092")
  .split(",")
  .map((broker) => broker.trim());

export const kafka = new Kafka({
  clientId: process.env.KAFKA_CLIENT_ID ?? "notification-engine",
  brokers,
});

export const kafkaProducer: Producer = kafka.producer({
  allowAutoTopicCreation: false,
});

export const eventsTopic =
  process.env.KAFKA_EVENTS_TOPIC ?? "notification.events";

let connected = false;

export async function connectKafka(): Promise<void> {
  if (connected) return;

  await kafkaProducer.connect();
  connected = true;

  console.log("Kafka producer connected");
}

export async function disconnectKafka(): Promise<void> {
  if (!connected) return;

  await kafkaProducer.disconnect();
  connected = false;
}