*# Event-Driven Notification Engine*



*A scalable, event-driven notification processing system built with Node.js, TypeScript, Fastify, PostgreSQL, Redis, Apache Kafka and RabbitMQ.*



*The system accepts application events, processes them asynchronously, applies notification policies and delivers notifications through multiple channels.*



*## 1. Features*



*- \*\*Transactional Outbox:\*\* Reliably publishes application events to Kafka.*

*- \*\*Idempotency:\*\* Prevents duplicate event processing using PostgreSQL, with Redis as an optimization.*

*- \*\*Event Processing:\*\* Processes events asynchronously using Kafka consumers.*

*- \*\*Message Deduplication:\*\* Uses an inbox table and database constraints to prevent duplicate processing.*

*- \*\*Multi-Channel Notifications:\*\* Supports Email, SMS, Push, WhatsApp and In-App notifications.*

*- \*\*Notification Preferences:\*\* Supports user channel preferences and consent checks.*

*- \*\*Quiet Hours:\*\* Defers eligible notifications during configured quiet hours.*

*- \*\*Frequency Capping:\*\* Limits notification frequency per user and channel.*

*- \*\*Critical Notifications:\*\* Supports priority-based handling and quiet-hour bypass for critical events.*

*- \*\*Template Management:\*\* Supports reusable, versioned and localized notification templates.*

*- \*\*Retry Mechanism:\*\* Implements retry scheduling and exponential backoff.*

*- \*\*Dead Letter Queue:\*\* Handles messages that cannot be delivered successfully.*

*- \*\*Provider Health:\*\* Tracks provider health and supports basic circuit-breaker behavior.*

*- \*\*Metrics:\*\* Exposes notification counts through a `/metrics` endpoint.*



*## 2. Technology Stack*



*| Technology | Purpose |*

*|---|---|*

*| Node.js | Runtime |*

*| TypeScript | Application development |*

*| Fastify | REST API |*

*| PostgreSQL | Persistent storage |*

*| Prisma | Database ORM |*

*| Redis | Idempotency optimization |*

*| Apache Kafka | Event streaming |*

*| RabbitMQ | Notification delivery queue |*

*| Docker | Infrastructure containers |*



*## 3. Architecture*



*The system follows an asynchronous event-driven architecture.*



*```text*

&#x20;                *Application*

&#x20;                     *|*

&#x20;                     *v*

&#x20;               *Fastify API*

&#x20;                     *|*

&#x20;                     *v*

&#x20;                 *PostgreSQL*

&#x20;                     *|*

&#x20;             *Transactional Outbox*

&#x20;                     *|*

&#x20;                     *v*

&#x20;                *Outbox Relay*

&#x20;                     *|*

&#x20;                     *v*

&#x20;                  *Kafka*

&#x20;                     *|*

&#x20;                     *v*

&#x20;               *Event Consumer*

&#x20;                     *|*

&#x20;            *Policy and Template*

&#x20;                 *Processing*

&#x20;                     *|*

&#x20;                     *v*

&#x20;                 *PostgreSQL*

&#x20;                     *|*

&#x20;                     *v*

&#x20;              *Notification*

&#x20;                *Dispatcher*

&#x20;                     *|*

&#x20;                     *v*

&#x20;                 *RabbitMQ*

&#x20;                     *|*

&#x20;                     *v*

&#x20;              *Delivery Worker*

&#x20;                     *|*

&#x20;         *+-----------+-----------+*

&#x20;         *|           |           |*

&#x20;        *Email       SMS         Push*

&#x20;         *|           |           |*

&#x20;      *WhatsApp    In-App      DLQ / Retry*

*```*



*Redis provides an optimization layer for idempotency. PostgreSQL remains the source of truth.*



*## 4. Notification Channels*



*The system supports five notification channels:*



*- EMAIL*

*- SMS*

*- PUSH*

*- WHATSAPP*

*- IN\_APP*



*\*\*Important:\*\* The current delivery implementations use mock providers. They demonstrate the notification processing and delivery lifecycle without sending real external messages.*



*## 5. Reliability and Processing*



*### Transactional Outbox*

*Events and their corresponding outbox records are written within the same database transaction.*



*### Idempotency*

*Repeated requests with the same idempotency key and payload return the existing result. Reusing a key with a different payload is rejected.*



*### Duplicate Message Protection*

*The processed-message inbox and database uniqueness constraints protect against duplicate event processing.*



*### Retry and Dead Letter Queue*

*Failed notification attempts can be retried with exponential backoff. Notifications that exhaust their retry budget are moved to the dead-letter state.*



*### Provider Health*

*The delivery worker tracks provider outcomes and implements basic circuit-breaker and recovery-probe behavior.*



*## 6. API Endpoints*



*| Method | Endpoint | Purpose |*

*|---|---|---|*

*| GET | `/health` | Health check |*

*| GET | `/metrics` | Notification metrics |*

*| POST | `/api/v1/users` | User management |*

*| POST | `/api/v1/events` | Submit application events |*



*Event submissions should include an `Idempotency-Key` header.*



*## 7. Project Setup*



*### Prerequisites*



*- Node.js*

*- npm*

*- Docker Desktop*

*- Git*



*### Install dependencies*



*```bash*

*npm install*

*```*



*### Configure environment*



*Create a local `.env` file using the required environment variables.*



*Configure the PostgreSQL, Redis, Kafka and RabbitMQ connection settings for your local Docker environment.*



*Do not commit real credentials or secrets.*



*### Start infrastructure*



*```bash*

*docker compose up -d*

*```*



*Ensure the required containers are healthy before starting the application.*



*### Generate Prisma Client*



*```bash*

*npx prisma generate*

*```*



*Apply the reviewed database migrations before running the application.*



*## 8. Running the Application*



*Start each process in a separate terminal from the project root.*



*\*\*Terminal 1 — API\*\**



*```bash*

*npm run dev*

*```*



*\*\*Terminal 2 — Outbox Relay\*\**



*```bash*

*npm run worker:outbox*

*```*



*\*\*Terminal 3 — Event Consumer\*\**



*```bash*

*npm run worker:consumer*

*```*



*\*\*Terminal 4 — Notification Dispatcher\*\**



*```bash*

*npm run worker:dispatcher*

*```*



*\*\*Terminal 5 — Delivery Worker\*\**



*```bash*

*npm run worker:delivery*

*```*



*## 9. Development Commands*



*| Command | Description |*

*|---|---|*

*| `npm run dev` | Start development API |*

*| `npm run build` | Compile TypeScript |*

*| `npm run typecheck` | Check TypeScript types |*

*| `npm start` | Start compiled API |*

*| `npm run worker:outbox` | Start outbox relay |*

*| `npm run worker:consumer` | Start Kafka consumer |*

*| `npm run worker:dispatcher` | Start notification dispatcher |*

*| `npm run worker:delivery` | Start delivery worker |*

*| `npm run worker:dlq:inspect` | Inspect dead-letter messages |*

*| `npm run worker:dlq:replay` | Replay eligible dead-letter messages |*



*## 10. Monitoring*



*The `/metrics` endpoint exposes notification counts grouped by status.*



*Example:*



*```text*

*notification\_engine\_notifications\_total{status="SENT"} 10*

*notification\_engine\_notifications\_total{status="SUPPRESSED"} 2*

*notification\_engine\_notifications\_total{status="DEAD\_LETTER"} 4*

*```*



*These values are examples from local development testing.*



*## 11. Verification*



*The following functionality has been verified in the local development environment:*



*- API event submission.*

*- Idempotency and duplicate request handling.*

*- Transactional outbox publishing.*

*- Kafka event processing.*

*- RabbitMQ notification dispatch.*

*- Inbox-based duplicate protection.*

*- Email and In-App mock delivery.*

*- SMS mock delivery.*

*- Push mock delivery.*

*- WhatsApp mock delivery.*

*- Notification retry and dead-letter handling.*

*- Quiet-hours scheduling and critical bypass.*

*- Consent and frequency-cap checks.*

*- Provider health and recovery behavior.*

*- Metrics endpoint.*



*TypeScript validation:*



*```bash*

*npm run typecheck*

*```*



*## 12. Current Limitations*



*- External email, SMS, push and WhatsApp provider integrations are not configured.*

*- DND registry lookup exists in the code, but DND-based notification suppression is currently disabled. Live TRAI/DLT integration and production messaging compliance are not implemented.*

*- Production deployment, distributed load testing and real provider failover require additional validation.*

*- The metrics endpoint currently exposes notification counts rather than a complete production observability suite.*

*- The npm test script is currently a placeholder; automated test coverage still needs to be added.*



*## 13. Future Improvements*



*- Integrate real notification providers.*

*- Implement production-grade DND and messaging compliance.*

*- Add automated integration and unit tests.*

*- Expand delivery analytics and observability.*

*- Add production deployment and load testing.*

*- Improve provider routing and multi-provider failover.*



*---*



*\*\*Project:\*\* Event-Driven Notification Engine*  

*\*\*Purpose:\*\* Asynchronous, reliable and scalable notification processing.*

