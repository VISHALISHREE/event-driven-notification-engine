import "dotenv/config";

import { EachMessagePayload } from "kafkajs";

import { prisma } from "../infra/database/prisma.js";
import { kafka, eventsTopic } from "../infra/kafka/kafka.producer.js";

import {
  EventProcessingStatus,
  Channel,
  ConsentPurpose,
  ConsentStatus,
  NotificationStatus,
} from "../generated/prisma/enums.js";

function getConsentPurpose(eventType: string): ConsentPurpose {
  switch (eventType) {
    case "LOGIN_ALERT":
    case "PASSWORD_CHANGED":
      return ConsentPurpose.SECURITY;

    case "REGULATORY_NOTICE":
      return ConsentPurpose.REGULATORY;

    default:
      return ConsentPurpose.TRANSACTIONAL;
  }
}

const CONSUMER_NAME = "notification-event-processor-v1";

const consumer = kafka.consumer({
  groupId: CONSUMER_NAME,
});

let shuttingDown = false;

interface EventMessage {
  eventId: string;
  externalEventId: string;
}

function getHeaderValue(
  value: string | Buffer | undefined
): string | undefined {
  if (value === undefined) return undefined;

  return Buffer.isBuffer(value)
    ? value.toString("utf8")
    : value;
}

function isUuid(value: unknown): value is string {
  return (
    typeof value === "string" &&
    /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value)
  );
}

function renderTemplate(
  template: string,
  variables: Record<string, unknown>
): string {
  return template.replace(
    /{{\s*([\w.]+)\s*}}/g,
    (_match, path: string) => {
      const value = path
        .split(".")
        .reduce<unknown>((current, key) => {
          if (
            current !== null &&
            typeof current === "object"
          ) {
            return (current as Record<string, unknown>)[key];
          }

          return undefined;
        }, variables);

      return value === null || value === undefined
        ? ""
        : String(value);
    }
  );
}

const CRITICAL_EVENT_TYPES = new Set<string>([
  "MARGIN_CALL",
  "PASSWORD_CHANGED",
  "LOGIN_ALERT",
  "REGULATORY_NOTICE",
  "TRANSACTION_FAILED",
  "SIP_FAILED",
  "ORDER_REJECTED",
  "KYC_REJECTED",
]);

function isCriticalEvent(eventType: string): boolean {
  return CRITICAL_EVENT_TYPES.has(eventType);
}

function parseTime(value: string | null | undefined): number | null {
  if (!value || !/^([01]\d|2[0-3]):[0-5]\d$/.test(value)) {
    return null;
  }

const hours = Number(value.slice(0, 2));
const minutes = Number(value.slice(3, 5));

return hours * 60 + minutes;
}

function getQuietHoursState(
  now: Date,
  enabled: boolean,
  start: string | null | undefined,
  end: string | null | undefined,
  timezone: string,
): { active: boolean; endAt: Date | null } {
  if (!enabled) {
    return { active: false, endAt: null };
  }

  const startMinutes = parseTime(start);
  const endMinutes = parseTime(end);

  if (
    startMinutes === null ||
    endMinutes === null ||
    startMinutes === endMinutes
  ) {
    return { active: false, endAt: null };
  }

  const formatter = new Intl.DateTimeFormat("en-GB", {
    timeZone: timezone,
    hour: "2-digit",
    minute: "2-digit",
    hourCycle: "h23",
  });

  const parts = formatter.formatToParts(now);

  const hours = Number(parts.find(p => p.type === "hour")?.value);
  const minutes = Number(parts.find(p => p.type === "minute")?.value);

  const currentMinutes = hours * 60 + minutes;

  const active =
    startMinutes < endMinutes
      ? currentMinutes >= startMinutes && currentMinutes < endMinutes
      : currentMinutes >= startMinutes || currentMinutes < endMinutes;

  if (!active) {
    return { active: false, endAt: null };
  }

  // Find the next occurrence of the quiet-hours ending time.
  const candidate = new Date(
    Math.floor(now.getTime() / 60000) * 60000 + 60000,
  );

  for (let i = 0; i < 26 * 60; i++) {
    const candidateParts = formatter.formatToParts(candidate);

    const candidateHour = Number(
      candidateParts.find(p => p.type === "hour")?.value,
    );

    const candidateMinute = Number(
      candidateParts.find(p => p.type === "minute")?.value,
    );

    if (
      candidateHour * 60 + candidateMinute === endMinutes
    ) {
      return { active: true, endAt: candidate };
    }

    candidate.setTime(candidate.getTime() + 60000);
  }

  throw new Error("Could not calculate quiet-hours ending time");
}

async function processMessage({
  topic,
  partition,
  message,
}: EachMessagePayload): Promise<void> {
  if (!message.value) {
    throw new Error("Kafka message has an empty value");
  }

  let event: EventMessage;

  try {
    event = JSON.parse(
      message.value.toString("utf8")
    ) as EventMessage;
  } catch {
    throw new Error("Invalid Kafka JSON");
  }

  if (!isUuid(event.eventId) || !event.externalEventId) {
    throw new Error("Invalid event identifiers");
  }

  const outboxMessageId = getHeaderValue(
    message.headers?.outboxMessageId as
      | string
      | Buffer
      | undefined
  );

  if (!outboxMessageId) {
    throw new Error("Missing outboxMessageId");
  }

  const result = await prisma.$transaction(async (tx) => {
    const existingEvent = await tx.event.findUnique({
      where: { id: event.eventId },
    });

    if (!existingEvent) {
      throw new Error(
        `Event not found: ${event.eventId}`
      );
    }

    // Inbox deduplication.
    const inbox = await tx.processedMessage.createMany({
      data: [{
        consumerName: CONSUMER_NAME,
        messageId: outboxMessageId,
      }],
      skipDuplicates: true,
    });

    if (inbox.count === 0) {
      return { duplicate: true, created: 0, suppressed: 0 };
    }

    if (
      existingEvent.status === EventProcessingStatus.PROCESSED
    ) {
      return { duplicate: false, created: 0, suppressed: 0 };
    }

    if (
      existingEvent.status !== EventProcessingStatus.RECEIVED &&
      existingEvent.status !== EventProcessingStatus.PROCESSING
    ) {
      throw new Error(
        `Cannot process event in status ${existingEvent.status}`
      );
    }

    const payload =
      existingEvent.payload &&
      typeof existingEvent.payload === "object" &&
      !Array.isArray(existingEvent.payload)
        ? existingEvent.payload as Record<string, unknown>
        : {};

    const userId = payload.userId;

    // Old test events may contain "test-user" instead of a UUID.
    if (!isUuid(userId)) {
      await tx.event.update({
        where: { id: event.eventId },
        data: {
          status: EventProcessingStatus.FAILED,
          processedAt: new Date(),
        },
      });

      return { duplicate: false, created: 0, suppressed: 0 };
    }

    const user = await tx.user.findUnique({
      where: { id: userId },
      include: { preference: true },
    });

    if (!user) {
      await tx.event.update({
        where: { id: event.eventId },
        data: {
          status: EventProcessingStatus.FAILED,
          processedAt: new Date(),
        },
      });

      return { duplicate: false, created: 0, suppressed: 0 };
    }

    if (!user.isActive) {
      await tx.event.update({
        where: { id: event.eventId },
        data: {
          status: EventProcessingStatus.PROCESSED,
          processedAt: new Date(),
        },
      });

      return { duplicate: false, created: 0, suppressed: 0 };
    }

    const preferredChannels =
      user.preference?.preferredChannels ?? [];

const channels = [
  Channel.EMAIL,
  Channel.SMS,
  Channel.IN_APP,
  Channel.PUSH,
  Channel.WHATSAPP,
].filter((channel) =>
  preferredChannels.includes(channel)
);

    let created = 0;
    let suppressed = 0;

    const now = new Date();

const quietHours = getQuietHoursState(
  now,
  user.preference?.quietHoursEnabled ?? false,
  user.preference?.quietHoursStart,
  user.preference?.quietHoursEnd,
  user.timezone,
);

const critical = isCriticalEvent(existingEvent.eventType);

    // Daily cap window (UTC).
    const dayStart = new Date(now);
    dayStart.setUTCHours(0, 0, 0, 0);

    for (const channel of channels) {
      // Find active template for this event and channel.
      const template = await tx.notificationTemplate.findFirst({
        where: {
          eventType: existingEvent.eventType,
          channel,
          isActive: true,
        },
        include: {
          versions: {
            where: { locale: user.locale },
            orderBy: { version: "desc" },
            take: 1,
          },
        },
      });

      let templateVersion = template?.versions[0];

      // Fallback to en-IN if user's locale is unavailable.
      if (!templateVersion && user.locale !== "en-IN") {
        const fallback = await tx.notificationTemplate.findFirst({
          where: {
            eventType: existingEvent.eventType,
            channel,
            isActive: true,
          },
          include: {
            versions: {
              where: { locale: "en-IN" },
              orderBy: { version: "desc" },
              take: 1,
            },
          },
        });

        templateVersion = fallback?.versions[0];
      }

      if (!templateVersion) {
        console.log(
          `No active template: ${existingEvent.eventType}/${channel}`
        );
        continue;
      }

      // Check the latest transactional consent.
      const consent = await tx.consentRecord.findFirst({
        where: {
          userId: user.id,
          channel,
          purpose: getConsentPurpose(existingEvent.eventType),
        },
        orderBy: { recordedAt: "desc" },
      });

      const hasConsent =
        consent?.status === ConsentStatus.GRANTED &&
        (!consent.expiresAt || consent.expiresAt > now);

        const hasValidPhone =
  channel !== Channel.SMS ||
  Boolean(user.phone && user.phone.trim().length >= 10);

  const isDndBlocked =
  channel === Channel.SMS && user.phone
    ? await tx.dndRegistry.findFirst({
        where: {
          phoneNumber: user.phone,
          isActive: true,
          OR: [
            { expiresAt: null },
            { expiresAt: { gt: now } },
          ],
        },
      })
    : null;

const dndAllowed = true;

      // Check channel-specific frequency cap.
      const channelCap =
        await tx.channelFrequencyCap.findUnique({
          where: {
            userId_channel: {
              userId: user.id,
              channel,
            },
          },
        });

      const dailyLimit =
        channelCap?.enabled
          ? channelCap.dailyLimit
          : user.preference?.dailyFrequencyCap ?? 0;

      const sentToday = await tx.notification.count({
        where: {
          userId: user.id,
          channel,
          createdAt: { gte: dayStart },
          status: {
            notIn: [
              NotificationStatus.SUPPRESSED,
              NotificationStatus.CANCELLED,
            ],
          },
        },
      });

      const withinLimit = sentToday < dailyLimit;

     const policyAllowed =
  hasConsent && withinLimit && hasValidPhone && dndAllowed;

const deferredForQuietHours =
  policyAllowed &&
  quietHours.active &&
  !critical;

      const variables: Record<string, unknown> = {
        ...payload,
        name: user.name,
      };

      const renderedBody = renderTemplate(
        templateVersion.body,
        variables
      );

      const renderedSubject = templateVersion.subject
        ? renderTemplate(templateVersion.subject, variables)
        : null;

      await tx.notification.create({
data: {
  userId: user.id,
  eventId: existingEvent.id,
  channel,

  status: policyAllowed
    ? NotificationStatus.PENDING
    : NotificationStatus.SUPPRESSED,

  priority: critical ? 1 : 5,

  scheduledAt: deferredForQuietHours
    ? quietHours.endAt
    : null,

  locale: templateVersion.locale,
  templateVersionId: templateVersion.id,
  renderedSubject,
  renderedBody,
},
      });

if (policyAllowed) {
  created++;
} else {
  suppressed++;
}
    }

    // Complete event processing.
    await tx.event.update({
      where: { id: existingEvent.id },
      data: {
        status: EventProcessingStatus.PROCESSED,
        processedAt: new Date(),
      },
    });

    return {
      duplicate: false,
      created,
      suppressed,
    };
  });

  if (result.duplicate) {
    console.log("Duplicate Kafka message ignored:", outboxMessageId);
  } else {
    console.log("Event processing completed:", event.eventId);
    console.log("Notifications created:", result.created);
    console.log("Notifications suppressed:", result.suppressed);
  }

  // Commit only after the database transaction succeeds.
  await consumer.commitOffsets([{
    topic,
    partition,
    offset: (BigInt(message.offset) + 1n).toString(),
  }]);
}

async function start(): Promise<void> {
  console.log("Starting Notification Event Processor...");

  await prisma.$connect();
  await consumer.connect();

  await consumer.subscribe({
    topic: eventsTopic,
    fromBeginning: true,
  });

  console.log("Connected to Kafka:", eventsTopic);

  await consumer.run({
    autoCommit: false,

    eachMessage: async (payload) => {
      if (shuttingDown) return;

      try {
        await processMessage(payload);
      } catch (error) {
        console.error("Event processing failed:", error);

        shuttingDown = true;
        process.exitCode = 1;

        // Do not commit this failed message.
        // It can be retried when the worker restarts.
        await consumer.stop();
      }
    },
  });
}

async function shutdown(): Promise<void> {
  if (shuttingDown) return;

  shuttingDown = true;

  console.log("Shutting down consumer...");

  try {
    await consumer.stop();
    await consumer.disconnect();
  } catch (error) {
    console.error("Kafka shutdown error:", error);
  }

  await prisma.$disconnect();

  console.log("Consumer stopped.");
}

process.once("SIGINT", () => {
  void shutdown();
});

process.once("SIGTERM", () => {
  void shutdown();
});

start().catch(async (error) => {
  console.error("Consumer startup failed:", error);
  process.exitCode = 1;

  try {
    await consumer.disconnect();
  } catch {
    // Ignore disconnect errors during startup failure.
  }

  await prisma.$disconnect();
});