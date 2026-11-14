import type { FastifyPluginAsync } from "fastify";
import { prisma } from "../../infra/database/prisma.js";

interface CreateUserBody {
  name: string;
  email?: string;
  phone?: string;
  locale?: string;
  timezone?: string;
}

const userRoutes: FastifyPluginAsync = async (app) => {

  // CREATE USER
  app.post<{ Body: CreateUserBody }>(
    "/",
    {
      schema: {
        body: {
          type: "object",
          required: ["name"],
          additionalProperties: false,
          properties: {
            name: { type: "string", minLength: 1 },
            email: { type: "string" },
            phone: { type: "string" },
            locale: { type: "string" },
            timezone: { type: "string" },
          },
        },
      },
    },
    async (request, reply) => {
      try {
        const user = await prisma.user.create({
          data: request.body,
        });

        return reply.code(201).send({
          message: "User created successfully",
          data: user,
        });

      } catch (error) {
        if (
          typeof error === "object" &&
          error !== null &&
          "code" in error &&
          error.code === "P2002"
        ) {
          return reply.code(409).send({
            message: "Email or phone number already exists",
          });
        }

        throw error;
      }
    }
  );

// GET ALL USERS
app.get("/", async (_request, reply) => {
  const users = await prisma.user.findMany({
    orderBy: {
      createdAt: "desc",
    },
    take: 50,
  });

  return reply.send({
    message: "Users retrieved successfully",
    count: users.length,
    data: users,
  });
});

// UPDATE USER
app.put<{
  Params: { id: string };
  Body: {
    name?: string;
    email?: string | null;
    phone?: string | null;
    locale?: string;
    timezone?: string;
  };
}>("/:id", {
  schema: {
    params: {
      type: "object",
      required: ["id"],
      properties: {
        id: { type: "string", format: "uuid" },
      },
    },
    body: {
      type: "object",
      minProperties: 1,
      additionalProperties: false,
      properties: {
        name: { type: "string", minLength: 1 },
        email: { type: ["string", "null"] },
        phone: { type: ["string", "null"] },
        locale: { type: "string" },
        timezone: { type: "string" },
      },
    },
  },
}, async (request, reply) => {
  try {
    const user = await prisma.user.update({
      where: { id: request.params.id },
      data: request.body,
    });

    return reply.send({
      message: "User updated successfully",
      data: user,
    });
  } catch (error) {
    if (
      typeof error === "object" &&
      error !== null &&
      "code" in error
    ) {
      if (error.code === "P2025") {
        return reply.code(404).send({ message: "User not found" });
      }

      if (error.code === "P2002") {
        return reply.code(409).send({
          message: "Email or phone number already exists",
        });
      }
    }

    throw error;
  }
});

// DEACTIVATE USER
app.patch<{ Params: { id: string } }>(
  "/:id/deactivate",
  {
    schema: {
      params: {
        type: "object",
        required: ["id"],
        properties: {
          id: { type: "string", format: "uuid" },
        },
      },
    },
  },
  async (request, reply) => {
    try {
      const user = await prisma.user.update({
        where: { id: request.params.id },
        data: { isActive: false },
      });

      return reply.send({
        message: "User deactivated successfully",
        data: user,
      });
    } catch (error) {
      if (
        typeof error === "object" &&
        error !== null &&
        "code" in error &&
        error.code === "P2025"
      ) {
        return reply.code(404).send({ message: "User not found" });
      }

      throw error;
    }
  }
);

  // GET USER BY ID
  app.get<{ Params: { id: string } }>(
    "/:id",
    {
      schema: {
        params: {
          type: "object",
          required: ["id"],
          properties: {
            id: {
              type: "string",
              format: "uuid",
            },
          },
        },
      },
    },
    async (request, reply) => {
      const user = await prisma.user.findUnique({
        where: {
          id: request.params.id,
        },
      });

      if (!user) {
        return reply.code(404).send({
          message: "User not found",
        });
      }

      return reply.send({
        message: "User retrieved successfully",
        data: user,
      });
    }
  );
};

export default userRoutes;