# Scalable AI Chat Backend with Experimentation Framework

A production-minded backend for an AI chat product that supports rapid A/B experimentation and continuous improvement using real-time user feedback.

This repo is designed as a **portfolio-grade** project: it demonstrates end-to-end service ownership (API, state, experimentation, pipelines, observability, and cloud deployment) using a pragmatic Python + Rust stack.

---

## Contents

- [What this is](#what-this-is)
- [Architecture](#architecture)
- [Repository layout](#repository-layout)
- [Key concepts](#key-concepts)
- [Local setup](#local-setup)
- [Running the system locally](#running-the-system-locally)
- [Configuration](#configuration)
- [Experimentation framework](#experimentation-framework)
- [Feedback + data pipeline](#feedback--data-pipeline)
- [Observability](#observability)
- [Cloud deployment](#cloud-deployment)
  - [AWS reference deployment (recommended)](#aws-reference-deployment-recommended)
  - [GCP reference deployment](#gcp-reference-deployment)
  - [Azure reference deployment](#azure-reference-deployment)
- [CI/CD with GitHub Actions](#cicd-with-github-actions)
- [Security and compliance basics](#security-and-compliance-basics)
- [Operations](#operations)
- [Troubleshooting](#troubleshooting)
- [Roadmap](#roadmap)

---

## What this is

**Project:** Scalable AI Chat Backend with Experimentation Framework

**Goal:** Build a production-grade backend system that serves an AI chat product, supports rapid A/B experimentation, and continuously improves responses using real-time user feedback.

**Tech stack:**
- **Python** (API + orchestration)
- **Rust** (performance-critical services)
- **PostgreSQL / Redis**
- **Message queue** (Kafka / PubSub / SQS)
- **Object storage** (S3 / GCS / Blob)
- **Basic observability** (metrics + tracing)

**What it does:**
- Serves chat requests
- Routes to an LLM
- Stores conversation state
- Runs multiple prompt/model variants via an experimentation framework
- Logs feedback signals
- Feeds signals into pipelines for analysis + iteration

---

## Architecture

High-level flow:

1. Client sends `POST /v1/chat`
2. API creates/updates a **conversation** and **message**
3. Experimentation service assigns a **variant** (prompt/model/tooling settings)
4. Orchestrator calls the LLM provider (or local model) and streams results
5. Response is persisted (and optionally cached)
6. Telemetry is emitted (logs/metrics/traces)
7. Feedback events (`thumbs_up/down`, edits, dwell time, report abuse, etc.) are recorded
8. Events are queued, batched, and written to object storage / warehouse for offline evaluation

**Reference components (can be swapped):**
- API + orchestrator: FastAPI / Starlette + async workers (Python)
- Experiment assignment: service + rule engine (Python or Rust)
- Streaming gateway / token fan-out: Rust (optional) for performance
- State: Postgres (source of truth) + Redis (cache, rate limiting)
- Queue: Kafka for local and high scale; SQS/PubSub as managed equivalents
- Batch pipeline: Spark/Beam/dbt (optional), or a simple consumer that writes Parquet

---

## Repository layout

This tree is a **recommended** structure for implementing the project. If you already have code, align it to this shape (or adjust paths below accordingly).

```text
.
├── README.md
├── LICENSE
├── .gitignore
├── .env.example
├── Makefile
├── docker-compose.yml
├── docs/
│   ├── architecture.md
│   ├── api.md
│   ├── experimentation.md
│   ├── data-pipeline.md
│   └── runbooks.md
├── api/                             # Python API + orchestration
│   ├── pyproject.toml
│   ├── src/
│   │   ├── app/
│   │   │   ├── main.py              # FastAPI entrypoint
│   │   │   ├── routes/
│   │   │   │   ├── chat.py
│   │   │   │   ├── feedback.py
│   │   │   │   └── health.py
│   │   │   ├── core/
│   │   │   │   ├── config.py
│   │   │   │   ├── logging.py
│   │   │   │   └── tracing.py
│   │   │   ├── domain/
│   │   │   │   ├── models.py         # Pydantic domain types
│   │   │   │   └── errors.py
│   │   │   ├── services/
│   │   │   │   ├── llm_router.py     # provider routing + retries
│   │   │   │   ├── experiments.py    # variant resolution
│   │   │   │   ├── conversations.py  # state persistence
│   │   │   │   ├── feedback.py
│   │   │   │   └── rate_limit.py
│   │   │   ├── infra/
│   │   │   │   ├── db.py             # SQLAlchemy/asyncpg
│   │   │   │   ├── redis.py
│   │   │   │   ├── queue.py          # Kafka/SQS/PubSub adapter
│   │   │   │   └── storage.py        # S3/GCS adapter
│   │   │   └── migrations/           # Alembic
│   ├── tests/
│   └── Dockerfile
├── services/
│   ├── rust-gateway/                 # optional token streaming / performance service
│   │   ├── Cargo.toml
│   │   ├── src/
│   │   └── Dockerfile
│   ├── experiment-svc/               # optional standalone experiment assignment
│   │   └── ...
│   └── worker/
│       ├── Dockerfile
│       └── src/                      # consumers / background jobs
├── infra/
│   ├── terraform/
│   │   ├── aws/
│   │   ├── gcp/
│   │   └── azure/
│   ├── k8s/
│   │   ├── base/
│   │   ├── overlays/
│   │   └── helm/
│   └── scripts/
│       ├── bootstrap_aws.sh
│       ├── bootstrap_gcp.sh
│       └── bootstrap_azure.sh
└── analytics/
    ├── dbt/                          # optional
    ├── notebooks/                    # optional
    └── schemas/                      # event + warehouse schemas
```

---

## Key concepts

### Conversations and messages
- A **conversation** is the long-lived thread.
- A **message** is one user/assistant turn.
- Store canonical state in Postgres. Cache hot reads in Redis.

### Experiments and variants
A **variant** is a stable bundle of:
- prompt template ID / version
- model name + provider
- temperature / top_p / max_tokens
- tools enabled / disabled
- safety settings
- retrieval settings (if using RAG)

The user experience should not “flip” mid-conversation unless you explicitly want it to.
For that reason, variant assignment should support:
- **sticky bucketing** per `(user_id, experiment_id)` or `(conversation_id, experiment_id)`
- optional overrides for internal users

### Feedback signals
Capture multiple signals; not all users will click thumbs.
Examples:
- thumbs up/down
- user edits/corrects response
- follow-up frustration (e.g., “that’s wrong”)
- dwell time / scroll depth
- report abuse / unsafe output
- “regenerate” clicks

---

## Local setup

### Prerequisites

- Docker + Docker Compose
- Python 3.11+ (if running outside Docker)
- Rust toolchain (optional, only if building Rust service locally)
- `make` (optional but helpful)

### Clone

```bash
git clone <your-repo-url>
cd <repo>
```

### Environment variables

Copy and edit:

```bash
cp .env.example .env
```

Minimum recommended entries:

```bash
# API
APP_ENV=local
API_HOST=0.0.0.0
API_PORT=8080

# Postgres
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=chat
POSTGRES_USER=chat
POSTGRES_PASSWORD=chat

# Redis
REDIS_HOST=redis
REDIS_PORT=6379

# Queue (choose one adapter)
QUEUE_BACKEND=kafka  # kafka|sqs|pubsub

# Kafka (local)
KAFKA_BROKERS=kafka:9092
KAFKA_TOPIC_EVENTS=chat-events

# Object storage (local = MinIO)
OBJECT_STORE_BACKEND=minio  # minio|s3|gcs|azure
MINIO_ENDPOINT=http://minio:9000
MINIO_ACCESS_KEY=minio
MINIO_SECRET_KEY=minio123
MINIO_BUCKET=chat-events

# LLM provider (choose one)
LLM_PROVIDER=openai  # openai|anthropic|azure_openai|local
LLM_API_KEY=__set_me__
LLM_MODEL=gpt-4o-mini

# Observability
OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317
LOG_LEVEL=INFO
```

> Tip: commit only `.env.example`, never `.env`.

### Start the stack

```bash
docker compose up -d --build
```

### Verify

- API health: `GET http://localhost:8080/health`
- Swagger UI: `http://localhost:8080/docs`
- Postgres: `localhost:5432`
- Redis: `localhost:6379`
- MinIO console: `http://localhost:9001`

---

## Running the system locally

### 1) Apply DB migrations

Inside the API container (or locally if you run Python locally):

```bash
docker compose exec api alembic upgrade head
```

### 2) Send a chat request

```bash
curl -X POST "http://localhost:8080/v1/chat" \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "u_123",
    "conversation_id": null,
    "message": "Explain rate limiting like I am a backend engineer.",
    "metadata": {"client": "curl"}
  }'
```

Expected behavior:
- A conversation is created
- A variant is assigned
- LLM is called
- Response is returned and stored

### 3) Submit feedback

```bash
curl -X POST "http://localhost:8080/v1/feedback" \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "u_123",
    "conversation_id": "c_abc",
    "message_id": "m_def",
    "rating": "thumbs_down",
    "reason": "hallucination",
    "comment": "Made up Redis commands"
  }'
```

### 4) Consume events (worker)

Workers read from the queue and write:
- raw JSON to object storage (good for replay)
- optionally Parquet to object storage for analytics

Example (inside worker container):

```bash
docker compose exec worker python -m worker.consume_events
```

---

## Configuration

### Service config model (recommended)

Use a typed config object that supports:
- env vars
- per-environment overrides
- validation on boot

Example pattern:

- `APP_ENV=local|dev|staging|prod`
- `CONFIG_FILE=./config/prod.yaml` (optional)

### Feature flags

Use a simple **flag service** (in-memory + DB) or external providers later.
Recommended flags:
- `chat.streaming.enabled`
- `tools.enabled`
- `safety.strict_mode`
- `experiments.enabled`
- `cache.enabled`

---

## Experimentation framework

### Goals
- Assign variants consistently and fairly
- Minimize p-hacking by logging exposure, assignment, and outcomes
- Enable fast iteration without redeploying code for every change

### Data model

**experiments**
- `id`, `name`, `status` (draft/running/paused)
- `allocation` rules (traffic split)
- `targeting` (country/app version/user cohort)

**variants**
- `id`, `experiment_id`, `name`
- `prompt_template_id`, `model`, `params_json`
- `tools_json`, `safety_json`, `retrieval_json`

**exposures**
- `experiment_id`, `variant_id`, `user_id`, `conversation_id`, `timestamp`

**outcomes**
- `variant_id`, `message_id`, `rating`, `latency_ms`, `token_usage`, etc.

### Assignment algorithm (simple + solid)

- Hash bucket with stickiness:
  - key: `user_id` (preferred), fallback `conversation_id`
  - bucket: `hash(key + experiment_salt) % 10000`
  - assign based on cumulative traffic weights
- Log exposure immediately on assignment
- Support overrides:
  - `X-Experiment-Override: experiment_a=variant_b`

### Practical safeguards

- **Sticky per conversation** unless explicitly testing within-conversation changes.
- **Exclude internal users** from experiments unless you want dogfooding data.
- **Ramp traffic**: 1% → 5% → 25% → 50% → 100%.
- **Kill switch**: instantly pause an experiment if error rate spikes.

---

## Feedback + data pipeline

### Event schema

Every event should include consistent metadata:

```json
{
  "event_id": "evt_...",
  "event_type": "chat.response|chat.feedback|chat.exposure",
  "ts": "2026-02-17T00:00:00Z",
  "user_id": "u_123",
  "conversation_id": "c_abc",
  "message_id": "m_def",
  "experiment_id": "exp_1",
  "variant_id": "var_2",
  "payload": {}
}
```

### Pipeline stages

1. **API emits events** (sync write to DB for critical records; async queue for analytics)
2. **Queue buffers** (Kafka/SQS/PubSub)
3. **Worker consumes** and writes:
   - `s3://<bucket>/raw/dt=YYYY-MM-DD/hour=HH/*.jsonl.gz`
   - `s3://<bucket>/parquet/dt=YYYY-MM-DD/*.parquet`
4. **Warehouse external tables** (optional):
   - Athena/Glue, BigQuery external tables, or Snowflake stages
5. **Analytics layer** (optional):
   - dbt models for experiment dashboards
   - evaluation jobs for offline scoring

---

## Observability

### Logs
- Structured JSON logs
- Include `request_id`, `conversation_id`, `experiment_id`, `variant_id`
- Redact secrets and PII

### Metrics
Recommended counters/histograms:
- `http_requests_total{route,code}`
- `http_request_duration_seconds{route}`
- `llm_latency_seconds{provider,model}`
- `llm_errors_total{provider,code}`
- `queue_publish_total`
- `queue_consume_lag_seconds`
- `db_query_duration_seconds`

### Tracing
- Use OpenTelemetry SDK in Python + Rust
- Propagate trace IDs to:
  - LLM calls
  - DB calls
  - queue publish/consume

---

## Cloud deployment

You can deploy this project multiple ways. The cleanest approach is:

- **Containerize everything**
- Use **managed Postgres** + **managed Redis**
- Use **managed queue**
- Use **object storage**
- Use **Kubernetes** (EKS/GKE/AKS) or **serverless containers** (ECS/Fargate, Cloud Run)

Below are reference deployments that match the repo structure in `infra/`.

### Cloud resources you will need (any provider)

- VPC / VNet networking with private subnets
- NAT or egress for outbound LLM provider calls
- Load balancer / ingress
- Managed Postgres
- Managed Redis
- Queue service
- Object storage bucket
- Secrets manager
- Observability (managed or self-hosted stack)

---

## AWS reference deployment (recommended)

This section assumes AWS because it maps cleanly to the components:
- API/worker: ECS Fargate or EKS
- Postgres: RDS (Aurora Postgres optional)
- Redis: ElastiCache
- Queue: SQS (simple) or MSK (Kafka)
- Object store: S3
- Secrets: Secrets Manager + KMS
- Observability: CloudWatch + X-Ray or OTEL → Grafana/Tempo

### Option A: ECS Fargate (simpler ops)

**Suggested AWS services:**
- VPC with public + private subnets
- Application Load Balancer (ALB)
- ECS Cluster with Fargate services:
  - `api` (desired count 2+)
  - `worker` (desired count 1+)
  - optional `rust-gateway`
- RDS Postgres in private subnets
- ElastiCache Redis in private subnets
- SQS queue + DLQ
- S3 bucket for events + lifecycle rules
- IAM roles for tasks (least privilege)
- Secrets Manager for `LLM_API_KEY`, DB creds (or use RDS IAM auth)

#### 1) Bootstrap AWS (Terraform)

From repo root:

```bash
cd infra/terraform/aws
terraform init
terraform plan -var="env=dev"
terraform apply -var="env=dev"
```

Terraform should create:
- VPC, subnets, route tables, security groups
- RDS instance + parameter group
- ElastiCache cluster
- SQS queue + DLQ
- S3 bucket
- ECS cluster + task execution roles
- ALB + listener rules
- ECR repositories

#### 2) Build + push images

Authenticate to ECR:

```bash
aws ecr get-login-password --region <region> \
  | docker login --username AWS --password-stdin <acct>.dkr.ecr.<region>.amazonaws.com
```

Build/push:

```bash
docker build -t api:dev ./api
docker tag api:dev <ecr>/api:dev
docker push <ecr>/api:dev

docker build -t worker:dev ./services/worker
docker tag worker:dev <ecr>/worker:dev
docker push <ecr>/worker:dev
```

#### 3) Configure secrets

Store secrets in AWS Secrets Manager:
- `LLM_API_KEY`
- `POSTGRES_PASSWORD` (or rotate via RDS)
- any provider keys

You then mount them as environment variables in ECS task definitions.

#### 4) Deploy services

If Terraform manages ECS services, update image tags and apply again.

If not, create ECS task definitions:
- CPU/memory sized for LLM concurrency
- env vars for DB/Redis/SQS/S3
- IAM task role permissions:
  - `s3:PutObject` on bucket prefix
  - `sqs:SendMessage` / `ReceiveMessage` / `DeleteMessage`
  - `secretsmanager:GetSecretValue`

#### 5) Run migrations

Run as a one-off task in ECS:

```bash
# example: run alembic inside the api image as a one-off ECS task
alembic upgrade head
```

#### 6) Validate

- ALB URL → `/health` returns 200
- `/docs` reachable (lock down in prod)
- Chat requests succeed
- Worker drains SQS and writes to S3

#### 7) Autoscaling

- API: scale on CPU + request count
- Worker: scale on SQS queue depth
- Redis/RDS: start modest, monitor and right-size

### Option B: EKS (more flexible)

Use EKS if you want:
- custom networking, service mesh
- fine-grained autoscaling (HPA/KEDA)
- consistent platform across projects

Recommended:
- Helm charts in `infra/k8s/helm/`
- External Secrets Operator for AWS Secrets Manager
- KEDA for SQS/Kafka lag-based autoscaling
- AWS Load Balancer Controller for ALB ingress

---

## GCP reference deployment

Mapping:
- API/worker: Cloud Run (simplest) or GKE
- Postgres: Cloud SQL
- Redis: Memorystore
- Queue: Pub/Sub
- Object store: GCS
- Secrets: Secret Manager
- Observability: Cloud Logging + Cloud Trace + OTEL

### Cloud Run approach

1. Build and push to Artifact Registry
2. Deploy `api` service to Cloud Run with min instances 1, max based on budget
3. Deploy `worker` as:
   - Cloud Run job (scheduled / triggered), or
   - Cloud Run service pulling from Pub/Sub (push subscription), or
   - GKE deployment for long-running consumers
4. Use Pub/Sub for events; worker writes to GCS

Key notes:
- Cloud Run egress to internet may need Serverless VPC Access if you want private IP routes
- Cloud SQL access typically uses Cloud SQL Auth Proxy / connector

---

## Azure reference deployment

Mapping:
- API/worker: Container Apps or AKS
- Postgres: Azure Database for PostgreSQL
- Redis: Azure Cache for Redis
- Queue: Service Bus
- Object store: Blob Storage
- Secrets: Key Vault
- Observability: Application Insights + OTEL

Container Apps is usually the fastest path for a portfolio repo.

---

## CI/CD with GitHub Actions

A solid baseline:
- Lint + tests on PRs
- Build images on merge to `main`
- Push to registry (ECR/Artifact Registry/ACR)
- Deploy via Terraform/HelM (or provider CLI)

Example workflow steps:
1. `python -m ruff check`
2. `pytest`
3. `cargo test` (if Rust enabled)
4. build Docker images
5. push to registry
6. deploy (dev/staging/prod gated)

Store secrets in GitHub:
- cloud credentials (use OIDC where possible)
- registry credentials if needed
- do **not** store LLM keys in GitHub if you can avoid it; prefer cloud secrets managers

---

## Security and compliance basics

- Never log prompts/responses with raw PII by default
- Redact or hash user identifiers for analytics
- Encrypt:
  - in transit (TLS everywhere)
  - at rest (KMS-managed keys for S3/GCS/Blob, RDS encryption)
- Rate limit:
  - per IP
  - per user_id
  - per API key (if you use keys)
- Abuse controls:
  - content moderation (provider or in-house)
  - blocklists and allowlists
  - feedback-based escalation

---

## Operations

### Runbooks (recommended to add under `docs/runbooks.md`)
- API down / 5xx spike
- LLM provider latency spike
- Queue backlog growth
- Redis memory pressure
- Postgres slow queries / connection exhaustion
- Experiment rollback

### Backups
- RDS automated backups + PITR
- Redis snapshotting (depending on criticality)
- S3 lifecycle + versioning (optional)

---

## Troubleshooting

**API container healthy but requests fail**
- confirm DB migrations ran
- check DB connectivity from API container
- verify `LLM_API_KEY` and provider base URL

**Worker not consuming**
- check queue backend config
- confirm topic/subscription exists
- verify IAM permissions (cloud) or broker connectivity (local)

**High latency**
- measure LLM latency vs app overhead
- enable caching for common prompts
- consider moving streaming/token relay to Rust gateway
- add connection pooling for Postgres

---

## Roadmap

- [ ] Add RAG (vector store + retriever) behind a feature flag
- [ ] Add offline eval job (golden set + regression detection)
- [ ] Add experiment dashboard (dbt + Superset/Metabase)
- [ ] Add KEDA autoscaling examples (Kafka/SQS)
- [ ] Add model routing policy (latency/cost/quality aware)

---

## License

Choose a license appropriate for your goals (MIT/Apache-2.0 are common for portfolio repos).
