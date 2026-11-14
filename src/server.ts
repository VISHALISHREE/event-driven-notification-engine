import Fastify from "fastify";
import { prisma } from "./infra/database/prisma.js";
import userRoutes from "./modules/users/user.routes.js";
import eventRoutes from "./modules/events/event.routes.js";

const app = Fastify({
  logger: true,
});

// Disconnect Prisma when Fastify shuts down.
app.addHook("onClose", async () => {
  await prisma.$disconnect();
});

app.register(userRoutes, { prefix: "/api/v1/users" });
app.register(eventRoutes, { prefix: "/api/v1/events" });

app.get("/health", async () => {
  return {
    status: "ok",
    service: "notification-engine",
  };
});

app.get("/metrics", async (_request, reply) => {
  const notificationCounts = await prisma.notification.groupBy({
    by: ["status"],
    _count: {
      _all: true,
    },
  });

  const metrics = [
    "# HELP notification_engine_notifications_total Total notifications by status",
    "# TYPE notification_engine_notifications_total gauge",
    ...notificationCounts.map(
      (item) =>
        `notification_engine_notifications_total{status="${item.status}"} ${item._count._all}`
    ),
  ];

  return reply
    .type("text/plain; version=0.0.4; charset=utf-8")
    .send(metrics.join("\n") + "\n");
});

const start = async () => {
  try {
    // Connect to PostgreSQL through Prisma.
    await prisma.$connect();

    app.log.info("Connected to PostgreSQL successfully");

    await app.listen({
      port: 3000,
      host: "0.0.0.0",
    });

    console.log("Notification Engine running on port 3000");
  } catch (error) {
    app.log.error(error);
    await prisma.$disconnect();
    process.exit(1);
  }
};

// Handle application shutdown safely.
process.once("SIGINT", () => {
  void app.close();
});

process.once("SIGTERM", () => {
  void app.close();
});

start();