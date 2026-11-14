-- CreateEnum
CREATE TYPE "Channel" AS ENUM ('SMS', 'EMAIL', 'PUSH', 'WHATSAPP', 'IN_APP');

-- CreateEnum
CREATE TYPE "EventType" AS ENUM ('TRANSACTION_SUCCESS', 'TRANSACTION_FAILED', 'LARGE_TRANSACTION', 'LOGIN_ALERT', 'PASSWORD_CHANGED', 'SIP_DUE', 'SIP_SUCCESS', 'SIP_FAILED', 'MARGIN_CALL', 'PRICE_ALERT', 'PORTFOLIO_UPDATE', 'ORDER_PLACED', 'ORDER_EXECUTED', 'ORDER_REJECTED', 'KYC_PENDING', 'KYC_APPROVED', 'KYC_REJECTED', 'DIVIDEND_CREDITED', 'IPO_UPDATE', 'REGULATORY_NOTICE', 'ACCOUNT_STATEMENT_READY', 'PAYMENT_DUE', 'WITHDRAWAL_SUCCESS', 'WITHDRAWAL_FAILED', 'DEPOSIT_SUCCESS', 'DEPOSIT_FAILED', 'SYSTEM_ANNOUNCEMENT');

-- CreateEnum
CREATE TYPE "EventProcessingStatus" AS ENUM ('RECEIVED', 'PROCESSING', 'PROCESSED', 'FAILED');

-- CreateEnum
CREATE TYPE "NotificationStatus" AS ENUM ('PENDING', 'QUEUED', 'PROCESSING', 'SENT', 'DELIVERED', 'READ', 'RETRY_SCHEDULED', 'FAILED', 'SUPPRESSED', 'DEFERRED', 'DEAD_LETTER', 'CANCELLED');

-- CreateEnum
CREATE TYPE "ConsentStatus" AS ENUM ('GRANTED', 'REVOKED');

-- CreateEnum
CREATE TYPE "ConsentSource" AS ENUM ('USER_ACTION', 'API', 'ADMIN', 'IMPORT');

-- CreateEnum
CREATE TYPE "ConsentPurpose" AS ENUM ('TRANSACTIONAL', 'MARKETING', 'SECURITY', 'REGULATORY');

-- CreateEnum
CREATE TYPE "ProviderStatus" AS ENUM ('ACTIVE', 'DEGRADED', 'DOWN', 'DISABLED');

-- CreateEnum
CREATE TYPE "DeliveryAttemptStatus" AS ENUM ('STARTED', 'ACCEPTED', 'DELIVERED', 'FAILED', 'UNKNOWN');

-- CreateEnum
CREATE TYPE "OutboxStatus" AS ENUM ('PENDING', 'PROCESSING', 'PUBLISHED', 'FAILED');

-- CreateEnum
CREATE TYPE "IdempotencyStatus" AS ENUM ('IN_PROGRESS', 'COMPLETED', 'FAILED');

-- CreateEnum
CREATE TYPE "DigestFrequency" AS ENUM ('DAILY', 'WEEKLY');

-- CreateEnum
CREATE TYPE "DigestDeliveryStatus" AS ENUM ('PENDING', 'PROCESSING', 'SENT', 'FAILED');

-- CreateEnum
CREATE TYPE "DeadLetterStatus" AS ENUM ('OPEN', 'REPLAY_PENDING', 'REPLAYED', 'RESOLVED', 'DISCARDED');

-- CreateTable
CREATE TABLE "User" (
    "id" UUID NOT NULL,
    "name" TEXT NOT NULL,
    "email" TEXT,
    "phone" TEXT,
    "locale" TEXT NOT NULL DEFAULT 'en',
    "timezone" TEXT NOT NULL DEFAULT 'Asia/Kolkata',
    "isActive" BOOLEAN NOT NULL DEFAULT true,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "User_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "UserPreference" (
    "id" UUID NOT NULL,
    "userId" UUID NOT NULL,
    "preferredChannels" "Channel"[] DEFAULT ARRAY[]::"Channel"[],
    "quietHoursEnabled" BOOLEAN NOT NULL DEFAULT false,
    "quietHoursStart" TEXT,
    "quietHoursEnd" TEXT,
    "digestEnabled" BOOLEAN NOT NULL DEFAULT false,
    "dailyFrequencyCap" INTEGER NOT NULL DEFAULT 5,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "UserPreference_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "ChannelFrequencyCap" (
    "id" UUID NOT NULL,
    "userId" UUID NOT NULL,
    "channel" "Channel" NOT NULL,
    "dailyLimit" INTEGER NOT NULL DEFAULT 5,
    "enabled" BOOLEAN NOT NULL DEFAULT true,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "ChannelFrequencyCap_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "ConsentRecord" (
    "id" UUID NOT NULL,
    "userId" UUID NOT NULL,
    "channel" "Channel" NOT NULL,
    "purpose" "ConsentPurpose" NOT NULL,
    "status" "ConsentStatus" NOT NULL,
    "source" "ConsentSource" NOT NULL,
    "evidenceRef" TEXT,
    "recordedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "expiresAt" TIMESTAMP(3),

    CONSTRAINT "ConsentRecord_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "DndRegistry" (
    "id" UUID NOT NULL,
    "phoneNumber" TEXT NOT NULL,
    "isActive" BOOLEAN NOT NULL DEFAULT true,
    "source" TEXT NOT NULL,
    "checkedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "sourceUpdatedAt" TIMESTAMP(3),
    "expiresAt" TIMESTAMP(3),
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "DndRegistry_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "Event" (
    "id" UUID NOT NULL,
    "producer" TEXT NOT NULL,
    "externalEventId" TEXT NOT NULL,
    "eventType" "EventType" NOT NULL,
    "schemaVersion" INTEGER NOT NULL DEFAULT 1,
    "payload" JSONB NOT NULL,
    "status" "EventProcessingStatus" NOT NULL DEFAULT 'RECEIVED',
    "occurredAt" TIMESTAMP(3) NOT NULL,
    "receivedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "processedAt" TIMESTAMP(3),

    CONSTRAINT "Event_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "Notification" (
    "id" UUID NOT NULL,
    "userId" UUID NOT NULL,
    "eventId" UUID NOT NULL,
    "channel" "Channel" NOT NULL,
    "status" "NotificationStatus" NOT NULL DEFAULT 'PENDING',
    "priority" INTEGER NOT NULL DEFAULT 5,
    "locale" TEXT NOT NULL DEFAULT 'en',
    "templateVersionId" UUID,
    "renderedSubject" TEXT,
    "renderedBody" TEXT,
    "scheduledAt" TIMESTAMP(3),
    "sentAt" TIMESTAMP(3),
    "deliveredAt" TIMESTAMP(3),
    "readAt" TIMESTAMP(3),
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,
    "retryCount" INTEGER NOT NULL DEFAULT 0,
    "maxRetries" INTEGER NOT NULL DEFAULT 3,
    "nextRetryAt" TIMESTAMP(3),
    "lastError" TEXT,

        CONSTRAINT "Notification_pkey" PRIMARY KEY ("id","createdAt")
) PARTITION BY RANGE ("createdAt");

-- Create monthly partitions from January 2026 through December 2027.
DO $$
DECLARE
    partition_start DATE := DATE '2026-01-01';
    partition_end DATE;
BEGIN
    WHILE partition_start < DATE '2028-01-01' LOOP
        partition_end := (partition_start + INTERVAL '1 month')::DATE;

        EXECUTE format(
            'CREATE TABLE %I PARTITION OF "Notification" FOR VALUES FROM (%L) TO (%L)',
            'Notification_' || to_char(partition_start, 'YYYY_MM'),
            partition_start,
            partition_end
        );

        partition_start := partition_end;
    END LOOP;
END $$;

-- Catch records outside the predefined monthly ranges.
CREATE TABLE "Notification_default"
PARTITION OF "Notification" DEFAULT;


-- CreateTable
CREATE TABLE "DeliveryAttempt" (
    "id" UUID NOT NULL,
    "notificationId" UUID NOT NULL,
    "notificationCreatedAt" TIMESTAMP(3) NOT NULL,
    "providerId" UUID NOT NULL,
    "attemptNumber" INTEGER NOT NULL,
    "status" "DeliveryAttemptStatus" NOT NULL DEFAULT 'STARTED',
    "providerMessageId" TEXT,
    "requestMetadata" JSONB,
    "responseMetadata" JSONB,
    "errorCode" TEXT,
    "errorMessage" TEXT,
    "startedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "completedAt" TIMESTAMP(3),
    "latencyMs" INTEGER,

    CONSTRAINT "DeliveryAttempt_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "DeliveryEvent" (
    "id" UUID NOT NULL,
    "notificationId" UUID NOT NULL,
    "notificationCreatedAt" TIMESTAMP(3) NOT NULL,
    "providerId" UUID,
    "providerEventId" TEXT,
    "eventType" TEXT NOT NULL,
    "payload" JSONB,
    "occurredAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "DeliveryEvent_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "NotificationTemplate" (
    "id" UUID NOT NULL,
    "key" TEXT NOT NULL,
    "eventType" "EventType" NOT NULL,
    "channel" "Channel" NOT NULL,
    "isActive" BOOLEAN NOT NULL DEFAULT true,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "NotificationTemplate_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "TemplateVersion" (
    "id" UUID NOT NULL,
    "templateId" UUID NOT NULL,
    "version" INTEGER NOT NULL,
    "locale" TEXT NOT NULL,
    "subject" TEXT,
    "body" TEXT NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "TemplateVersion_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "Provider" (
    "id" UUID NOT NULL,
    "name" TEXT NOT NULL,
    "channel" "Channel" NOT NULL,
    "status" "ProviderStatus" NOT NULL DEFAULT 'ACTIVE',
    "priority" INTEGER NOT NULL DEFAULT 5,
    "credentialRef" TEXT,
    "supportsIdempotency" BOOLEAN NOT NULL DEFAULT false,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "Provider_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "ProviderHealth" (
    "id" UUID NOT NULL,
    "providerId" UUID NOT NULL,
    "status" "ProviderStatus" NOT NULL,
    "successRate" DECIMAL(5,4) NOT NULL,
    "avgLatencyMs" INTEGER,
    "costPerRequest" DECIMAL(12,6),
    "totalRequests" INTEGER NOT NULL DEFAULT 0,
    "failedRequests" INTEGER NOT NULL DEFAULT 0,
    "checkedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "ProviderHealth_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "RoutingDecision" (
    "id" UUID NOT NULL,
    "notificationId" UUID NOT NULL,
    "notificationCreatedAt" TIMESTAMP(3) NOT NULL,
    "selectedProviderId" UUID,
    "selectedChannel" "Channel" NOT NULL,
    "policyVersion" TEXT NOT NULL,
    "reason" TEXT NOT NULL,
    "decisionFactors" JSONB NOT NULL,
    "alternatives" JSONB,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "RoutingDecision_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "IdempotencyKey" (
    "id" UUID NOT NULL,
    "scope" TEXT NOT NULL,
    "key" TEXT NOT NULL,
    "requestHash" TEXT NOT NULL,
    "status" "IdempotencyStatus" NOT NULL DEFAULT 'IN_PROGRESS',
    "responseBody" JSONB,
    "statusCode" INTEGER,
    "expiresAt" TIMESTAMP(3) NOT NULL,
    "lockedUntil" TIMESTAMP(3),
    "leaseToken" UUID,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "IdempotencyKey_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "OutboxMessage" (
    "id" UUID NOT NULL,
    "eventId" UUID,
    "aggregateType" TEXT NOT NULL,
    "aggregateId" TEXT NOT NULL,
    "topic" TEXT NOT NULL,
    "payload" JSONB NOT NULL,
    "status" "OutboxStatus" NOT NULL DEFAULT 'PENDING',
    "attempts" INTEGER NOT NULL DEFAULT 0,
    "availableAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "lockedUntil" TIMESTAMP(3),
    "leaseToken" UUID,
    "leaseOwner" TEXT,
    "lastAttemptAt" TIMESTAMP(3),
    "publishedAt" TIMESTAMP(3),
    "lastError" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "OutboxMessage_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "ProcessedMessage" (
    "id" UUID NOT NULL,
    "consumerName" TEXT NOT NULL,
    "messageId" TEXT NOT NULL,
    "processedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "ProcessedMessage_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "DeadLetterRecord" (
    "id" UUID NOT NULL,
    "source" TEXT NOT NULL,
    "messageId" TEXT NOT NULL,
    "payload" JSONB NOT NULL,
    "reason" TEXT NOT NULL,
    "attempts" INTEGER NOT NULL DEFAULT 0,
    "status" "DeadLetterStatus" NOT NULL DEFAULT 'OPEN',
    "replayCount" INTEGER NOT NULL DEFAULT 0,
    "lastReplayAt" TIMESTAMP(3),
    "resolutionNote" TEXT,
    "failedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "resolvedAt" TIMESTAMP(3),

    CONSTRAINT "DeadLetterRecord_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "DigestSchedule" (
    "id" UUID NOT NULL,
    "userId" UUID NOT NULL,
    "frequency" "DigestFrequency" NOT NULL,
    "weekday" INTEGER,
    "localTime" TEXT NOT NULL,
    "timezone" TEXT NOT NULL,
    "isEnabled" BOOLEAN NOT NULL DEFAULT true,
    "nextRunAt" TIMESTAMP(3),
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "DigestSchedule_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "DigestDelivery" (
    "id" UUID NOT NULL,
    "scheduleId" UUID NOT NULL,
    "status" "DigestDeliveryStatus" NOT NULL DEFAULT 'PENDING',
    "sentAt" TIMESTAMP(3),
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "DigestDelivery_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "DigestDeliveryItem" (
    "id" UUID NOT NULL,
    "digestDeliveryId" UUID NOT NULL,
    "notificationId" UUID NOT NULL,
    "notificationCreatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "DigestDeliveryItem_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "NotificationMetric" (
    "id" UUID NOT NULL,
    "metricDate" DATE NOT NULL,
    "channel" "Channel" NOT NULL,
    "providerId" UUID,
    "sentCount" INTEGER NOT NULL DEFAULT 0,
    "deliveredCount" INTEGER NOT NULL DEFAULT 0,
    "failedCount" INTEGER NOT NULL DEFAULT 0,
    "avgLatencyMs" INTEGER,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "NotificationMetric_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "User_email_key" ON "User"("email");

-- CreateIndex
CREATE UNIQUE INDEX "User_phone_key" ON "User"("phone");

-- CreateIndex
CREATE INDEX "User_isActive_idx" ON "User"("isActive");

-- CreateIndex
CREATE UNIQUE INDEX "UserPreference_userId_key" ON "UserPreference"("userId");

-- CreateIndex
CREATE INDEX "ChannelFrequencyCap_userId_enabled_idx" ON "ChannelFrequencyCap"("userId", "enabled");

-- CreateIndex
CREATE UNIQUE INDEX "ChannelFrequencyCap_userId_channel_key" ON "ChannelFrequencyCap"("userId", "channel");

-- CreateIndex
CREATE INDEX "ConsentRecord_userId_channel_purpose_recordedAt_idx" ON "ConsentRecord"("userId", "channel", "purpose", "recordedAt");

-- CreateIndex
CREATE INDEX "ConsentRecord_userId_channel_purpose_status_idx" ON "ConsentRecord"("userId", "channel", "purpose", "status");

-- CreateIndex
CREATE UNIQUE INDEX "DndRegistry_phoneNumber_key" ON "DndRegistry"("phoneNumber");

-- CreateIndex
CREATE INDEX "DndRegistry_isActive_expiresAt_idx" ON "DndRegistry"("isActive", "expiresAt");

-- CreateIndex
CREATE INDEX "Event_eventType_receivedAt_idx" ON "Event"("eventType", "receivedAt");

-- CreateIndex
CREATE INDEX "Event_status_receivedAt_idx" ON "Event"("status", "receivedAt");

-- CreateIndex
CREATE UNIQUE INDEX "Event_producer_externalEventId_key" ON "Event"("producer", "externalEventId");

-- CreateIndex
CREATE INDEX "Notification_userId_createdAt_idx" ON "Notification"("userId", "createdAt");

-- CreateIndex
CREATE INDEX "Notification_status_scheduledAt_idx" ON "Notification"("status", "scheduledAt");

-- CreateIndex
CREATE INDEX "Notification_status_nextRetryAt_idx" ON "Notification"("status", "nextRetryAt");

-- CreateIndex
CREATE INDEX "Notification_eventId_idx" ON "Notification"("eventId");

-- CreateIndex
CREATE INDEX "Notification_channel_status_idx" ON "Notification"("channel", "status");

-- CreateIndex
CREATE INDEX "DeliveryAttempt_providerId_status_idx" ON "DeliveryAttempt"("providerId", "status");

-- CreateIndex
CREATE INDEX "DeliveryAttempt_notificationId_notificationCreatedAt_idx" ON "DeliveryAttempt"("notificationId", "notificationCreatedAt");

-- CreateIndex
CREATE UNIQUE INDEX "DeliveryAttempt_notificationId_notificationCreatedAt_attemp_key" ON "DeliveryAttempt"("notificationId", "notificationCreatedAt", "attemptNumber");

-- CreateIndex
CREATE INDEX "DeliveryEvent_notificationId_notificationCreatedAt_occurred_idx" ON "DeliveryEvent"("notificationId", "notificationCreatedAt", "occurredAt");

-- CreateIndex
CREATE UNIQUE INDEX "DeliveryEvent_providerId_providerEventId_key" ON "DeliveryEvent"("providerId", "providerEventId");

-- CreateIndex
CREATE UNIQUE INDEX "NotificationTemplate_key_channel_key" ON "NotificationTemplate"("key", "channel");

-- CreateIndex
CREATE UNIQUE INDEX "TemplateVersion_templateId_version_locale_key" ON "TemplateVersion"("templateId", "version", "locale");

-- CreateIndex
CREATE INDEX "Provider_channel_status_idx" ON "Provider"("channel", "status");

-- CreateIndex
CREATE UNIQUE INDEX "Provider_name_channel_key" ON "Provider"("name", "channel");

-- CreateIndex
CREATE INDEX "ProviderHealth_providerId_checkedAt_idx" ON "ProviderHealth"("providerId", "checkedAt");

-- CreateIndex
CREATE INDEX "RoutingDecision_notificationId_notificationCreatedAt_create_idx" ON "RoutingDecision"("notificationId", "notificationCreatedAt", "createdAt");

-- CreateIndex
CREATE INDEX "RoutingDecision_selectedProviderId_idx" ON "RoutingDecision"("selectedProviderId");

-- CreateIndex
CREATE INDEX "IdempotencyKey_expiresAt_idx" ON "IdempotencyKey"("expiresAt");

-- CreateIndex
CREATE UNIQUE INDEX "IdempotencyKey_scope_key_key" ON "IdempotencyKey"("scope", "key");

-- CreateIndex
CREATE INDEX "OutboxMessage_status_availableAt_idx" ON "OutboxMessage"("status", "availableAt");

-- CreateIndex
CREATE INDEX "OutboxMessage_lockedUntil_idx" ON "OutboxMessage"("lockedUntil");

-- CreateIndex
CREATE UNIQUE INDEX "ProcessedMessage_consumerName_messageId_key" ON "ProcessedMessage"("consumerName", "messageId");

-- CreateIndex
CREATE INDEX "DeadLetterRecord_source_failedAt_idx" ON "DeadLetterRecord"("source", "failedAt");

-- CreateIndex
CREATE INDEX "DeadLetterRecord_status_failedAt_idx" ON "DeadLetterRecord"("status", "failedAt");

-- CreateIndex
CREATE INDEX "DeadLetterRecord_source_messageId_idx" ON "DeadLetterRecord"("source", "messageId");

-- CreateIndex
CREATE INDEX "DigestSchedule_isEnabled_nextRunAt_idx" ON "DigestSchedule"("isEnabled", "nextRunAt");

-- CreateIndex
CREATE INDEX "DigestDelivery_scheduleId_createdAt_idx" ON "DigestDelivery"("scheduleId", "createdAt");

-- CreateIndex
CREATE INDEX "DigestDeliveryItem_notificationId_notificationCreatedAt_idx" ON "DigestDeliveryItem"("notificationId", "notificationCreatedAt");

-- CreateIndex
CREATE UNIQUE INDEX "DigestDeliveryItem_digestDeliveryId_notificationId_notifica_key" ON "DigestDeliveryItem"("digestDeliveryId", "notificationId", "notificationCreatedAt");

-- CreateIndex
CREATE INDEX "NotificationMetric_metricDate_channel_idx" ON "NotificationMetric"("metricDate", "channel");

-- CreateIndex
CREATE INDEX "NotificationMetric_providerId_metricDate_idx" ON "NotificationMetric"("providerId", "metricDate");

-- AddForeignKey
ALTER TABLE "UserPreference" ADD CONSTRAINT "UserPreference_userId_fkey" FOREIGN KEY ("userId") REFERENCES "User"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "ChannelFrequencyCap" ADD CONSTRAINT "ChannelFrequencyCap_userId_fkey" FOREIGN KEY ("userId") REFERENCES "User"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "ConsentRecord" ADD CONSTRAINT "ConsentRecord_userId_fkey" FOREIGN KEY ("userId") REFERENCES "User"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "Notification" ADD CONSTRAINT "Notification_userId_fkey" FOREIGN KEY ("userId") REFERENCES "User"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "Notification" ADD CONSTRAINT "Notification_eventId_fkey" FOREIGN KEY ("eventId") REFERENCES "Event"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "Notification" ADD CONSTRAINT "Notification_templateVersionId_fkey" FOREIGN KEY ("templateVersionId") REFERENCES "TemplateVersion"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "DeliveryAttempt" ADD CONSTRAINT "DeliveryAttempt_notificationId_notificationCreatedAt_fkey" FOREIGN KEY ("notificationId", "notificationCreatedAt") REFERENCES "Notification"("id", "createdAt") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "DeliveryAttempt" ADD CONSTRAINT "DeliveryAttempt_providerId_fkey" FOREIGN KEY ("providerId") REFERENCES "Provider"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "DeliveryEvent" ADD CONSTRAINT "DeliveryEvent_notificationId_notificationCreatedAt_fkey" FOREIGN KEY ("notificationId", "notificationCreatedAt") REFERENCES "Notification"("id", "createdAt") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "DeliveryEvent" ADD CONSTRAINT "DeliveryEvent_providerId_fkey" FOREIGN KEY ("providerId") REFERENCES "Provider"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "TemplateVersion" ADD CONSTRAINT "TemplateVersion_templateId_fkey" FOREIGN KEY ("templateId") REFERENCES "NotificationTemplate"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "ProviderHealth" ADD CONSTRAINT "ProviderHealth_providerId_fkey" FOREIGN KEY ("providerId") REFERENCES "Provider"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "RoutingDecision" ADD CONSTRAINT "RoutingDecision_notificationId_notificationCreatedAt_fkey" FOREIGN KEY ("notificationId", "notificationCreatedAt") REFERENCES "Notification"("id", "createdAt") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "RoutingDecision" ADD CONSTRAINT "RoutingDecision_selectedProviderId_fkey" FOREIGN KEY ("selectedProviderId") REFERENCES "Provider"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "OutboxMessage" ADD CONSTRAINT "OutboxMessage_eventId_fkey" FOREIGN KEY ("eventId") REFERENCES "Event"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "DigestSchedule" ADD CONSTRAINT "DigestSchedule_userId_fkey" FOREIGN KEY ("userId") REFERENCES "User"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "DigestDelivery" ADD CONSTRAINT "DigestDelivery_scheduleId_fkey" FOREIGN KEY ("scheduleId") REFERENCES "DigestSchedule"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "DigestDeliveryItem" ADD CONSTRAINT "DigestDeliveryItem_digestDeliveryId_fkey" FOREIGN KEY ("digestDeliveryId") REFERENCES "DigestDelivery"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "DigestDeliveryItem" ADD CONSTRAINT "DigestDeliveryItem_notificationId_notificationCreatedAt_fkey" FOREIGN KEY ("notificationId", "notificationCreatedAt") REFERENCES "Notification"("id", "createdAt") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "NotificationMetric" ADD CONSTRAINT "NotificationMetric_providerId_fkey" FOREIGN KEY ("providerId") REFERENCES "Provider"("id") ON DELETE SET NULL ON UPDATE CASCADE;
