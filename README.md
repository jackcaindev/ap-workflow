# Freight AP Workflow

Freight AP Workflow automates accounts payable reconciliation for freight carriers. The system ingests invoices, bills of lading (BOLs), and proofs of delivery (PODs) from Gmail, extracts structured data from each document using Claude vision, assembles shipments by load number, and reconciles invoice amounts and supporting paperwork against rate confirmations. When reconciliation fails or data is ambiguous, exceptions are flagged for human review before invoices are approved or rejected.

## Architecture

The backend is a **FastAPI** service that orchestrates document processing through a **LangGraph** state machine with PostgreSQL-backed checkpoints. **PostgreSQL** stores shipments, documents, rate confirmations, reconciliation results, and workflow state. **Redis Streams** decouple Gmail polling from invoice processing with acknowledged consumer-group delivery.

On startup, the API runs three background tasks: a Gmail poller that enqueues attachment jobs every five minutes, an invoice worker that consumes stream deliveries and runs the LangGraph workflow, and a missing-document SLA scanner. The scanner evaluates shipment aggregates without Claude, uses PostgreSQL row locks for concurrent safety, and records shipment-owned exception transitions. A **React** UI (Vite + Tailwind) talks to the API for shipment dashboards, reconciliation detail, and human-in-the-loop approval.

```
Gmail Inbox
    │
    ▼
Gmail Poller ──XADD──▶ Redis Stream (freight-ap:invoice-jobs:v1)
                              │
                              ▼
                        Invoice Worker
                              │
                    ┌─────────┴─────────┐
                    ▼                   ▼
              LangGraph Workflow    Batch Email
           (extract → match → HITL)   Notifier
                    │
                    ▼
              PostgreSQL
         (shipments, docs, checkpoints)
                    ▲
                    │
              React UI (HITL review)
```

## Features

- Gmail ingestion with an acknowledged Redis Streams consumer group
- Claude vision extraction for invoices, BOLs, and PODs
- Automatic shipment assembly by load number
- Full reconciliation: amount variance, carrier match, BOL pickup date, POD confirmation
- Human-in-the-loop review for exceptions via LangGraph interrupt
- Batch email summary notifications
- React UI with shipment dashboard, reconciliation detail, and HITL approval
- Explicit DLQ listing, idempotent replay, queue metrics, and age-based retention controls
- Backend suite covering extraction, workflow, reconciliation, shipments, and reliable delivery

## Local Development

### Prerequisites

- Docker and Docker Compose
- Node.js 18+ (for the React UI)
- Python 3.12+ (for running the Gmail OAuth flow locally)
- An [Anthropic API key](https://console.anthropic.com/)
- A Google Cloud project with the Gmail API enabled and OAuth desktop credentials (`credentials.json`)

### Setup

1. Clone the repo:

   ```bash
   git clone <repo-url>
   cd ap-workflow
   ```

2. Copy `.env.example` to `.env` and fill in `ANTHROPIC_API_KEY`:

   ```bash
   cp .env.example .env
   ```

3. **Gmail OAuth setup** — place your Google OAuth `credentials.json` in the project root, then run the interactive auth flow locally (on the host, not inside Docker) to generate `token.json`:

   ```bash
   python3 -c "
   from google_auth_oauthlib.flow import InstalledAppFlow
   SCOPES = ['https://www.googleapis.com/auth/gmail.modify']
   flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
   creds = flow.run_local_server(port=0)
   with open('token.json', 'w') as f:
       f.write(creds.to_json())
   "
   ```

   This opens a browser for consent and writes `token.json`. Docker Compose mounts both files into the API container.

4. Start the backend stack:

   ```bash
   docker compose up --build
   ```

5. Run database migrations:

   ```bash
   docker compose exec api alembic upgrade head
   ```

6. Start the frontend (runs on http://localhost:5173):

   ```bash
   cd frontend && npm install && npm run dev
   ```

7. Seed a rate confirmation so incoming invoices have something to reconcile against:

   ```bash
   curl -X POST http://localhost:8000/rate-confirmations \
     -H "Content-Type: application/json" \
     -d '{
       "load_number": "LD-001",
       "carrier_name": "ACME FREIGHT",
       "origin": "Chicago, IL",
       "destination": "Dallas, TX",
       "agreed_rate": 1500.00,
       "currency": "USD",
       "shipment_date": "2026-01-15"
     }'
   ```

## Testing

The default suite keeps the existing fast tests and fake-Redis worker tests. Run it
against the development stack with:

```bash
docker compose exec api pytest -m "not integration" tests/ -q
```

The recovery suite uses its own PostgreSQL and Redis containers, random loopback
ports, a unique Compose project, and disposable volumes. Redis is configured with
AOF and `appendfsync=always`; one test restarts that isolated Redis container.

```bash
./scripts/test-integration.sh
```

Pass normal pytest selectors after the script to run a focused scenario:

```bash
./scripts/test-integration.sh tests/integration/test_delivery_recovery.py -k reclaim
```

The integration suite requires Docker Compose v2, Python 3.12, and `uv`. It must
run serially (do not use pytest-xdist) because the AOF test restarts Redis. A warm
run normally takes 25–60 seconds; the first run can take longer while images and
Python dependencies download. The runner removes its containers, network, and
volumes on exit, including after a test failure.

## Project Structure

```
app/
├── main.py                  # FastAPI app, CORS, lifespan (Gmail poller + invoice worker)
├── database.py              # Async SQLAlchemy engine and session factory
├── api/
│   └── routes/
│       ├── extraction.py    # POST /extract — standalone document extraction
│       ├── gmail.py         # GET /gmail/auth, POST /gmail/poll
│       ├── notifications.py # GET /notifications — batch email history
│       ├── rate_confirmations.py  # CRUD for rate confirmations
│       ├── shipments.py     # Shipment list, detail, and carrier analytics
│       └── workflow.py      # Workflow run, resume (HITL), and run listing
├── core/
│   └── config.py            # Pydantic settings from environment variables
├── models/
│   ├── base.py              # SQLAlchemy declarative base
│   ├── document.py          # Extracted document records
│   ├── notification.py      # Batch summary email audit log
│   ├── rate_confirmation.py # Agreed rates keyed by load number
│   ├── reconciliation_result.py  # Per-shipment check results
│   ├── shipment.py          # Shipment assembly and doc presence flags
│   └── workflow_run.py      # LangGraph run tracking and interrupt payloads
├── schemas/
│   └── extraction.py        # Pydantic schemas per document type
├── services/
│   ├── extraction.py        # Claude vision classification and extraction
│   ├── gmail_auth.py        # Gmail OAuth and service client
│   ├── gmail_poller.py      # Inbox polling and Redis Streams enqueue
│   ├── invoice_queue.py     # Stream/group, ACK, recovery, and DLQ operations
│   ├── invoice_worker.py    # Consumer-group delivery and LangGraph invocation
│   ├── matching.py          # Invoice-to-rate-confirmation amount matching
│   ├── notifier.py          # Batch summary email via Gmail
│   ├── reconciliation.py    # Full shipment reconciliation checks
│   └── shipment.py          # Shipment upsert and document linking
└── workflow/
    ├── graph.py             # LangGraph state graph and Postgres checkpointer
    ├── nodes.py             # Extract, match, exception (interrupt), approve/reject
    └── state.py             # TypedDict workflow state with message reducer
```

## Key Design Decisions

**LangGraph for human-in-the-loop.** Reconciliation exceptions need to pause processing until an AP clerk approves or rejects the variance. LangGraph's `interrupt()` mechanism checkpoints workflow state in PostgreSQL and resumes via `Command(resume=...)` when the UI calls `POST /workflow/{run_id}/resume`. This gives durable, restart-safe HITL without building custom pause/resume infrastructure — the graph encodes the happy path and exception branches, and the checkpointer preserves state across API restarts.

**Redis Streams provide recoverable at-least-once delivery.** Each Gmail MIME attachment gets a stable key from its mailbox, message ID, and MIME part ID, then is atomically deduplicated and appended to the stream. A consumer acknowledges the entry only after its workflow state commits to PostgreSQL. If the worker exits first, the entry stays pending and another consumer reclaims it after the visibility timeout.

Delivery is at least once, not exactly once: PostgreSQL and Redis do not share a transaction. PostgreSQL therefore enforces a unique attachment source key and deterministic workflow run ID, so redelivery after a database commit but before `XACK` reuses the existing document and workflow. Reconciliation results and workflow audit events are idempotent per run. Active jobs heartbeat their leases; transient failures remain pending and are reclaimed up to `INVOICE_MAX_ATTEMPTS` (three by default). The final failed attempt and malformed payloads are copied to `freight-ap:invoice-jobs:dlq:v1`, including their original fields and failure reason, before the source entry is acknowledged.

The Queue Operations API and React view are intentionally unauthenticated controls for
the trusted, single-user local demo. Operators can list retained DLQ evidence, request an
idempotent replay with a UUID request key, inspect queue metrics, or explicitly purge
entries older than a supplied cutoff. Replay preserves the original source identity and
bypasses only the potentially stale Redis enqueue-dedupe key; enqueueing or acknowledging
a replay does not prove successful business processing, approval, posting, or payment.

Reads are bounded by `MAX_BATCH_SIZE`, but each entry is processed, acknowledged, retried, or dead-lettered independently. One failed attachment therefore does not block its siblings. Docker Compose enables Redis append-only persistence. Queue age is available from `enqueued_at`, Redis pending delivery count is the attempt count, and retry metadata retains the latest failure reason.

**Partial reconciliation on incomplete document sets.** Freight paperwork arrives asynchronously — an invoice may show up days before the BOL or POD. Rather than waiting for all four documents, reconciliation runs on whatever is present. Invoice, rate confirmation, BOL, and POD presence share the configurable `MISSING_DOCUMENT_SLA_HOURS` grace period (72 hours by default). Missing evidence is `not_evaluated` while within grace and becomes an explicit shipment-level overdue exception after the SLA. `MISSING_DOCUMENT_SCAN_INTERVAL_SECONDS` controls the best-effort in-process scanner cadence.

**Business state is multidimensional.** Workflow processing (`pending` through `complete` or `failed`), shipment reconciliation (`pending`, `partial`, `reconciled`, or `exception`), immutable human disposition (`approved` or `rejected`), and downstream posting/payment state are persisted separately. An approved exception remains an exceptional reconciliation with an explicit business override; it becomes `ready_for_posting` without pretending the shipment reconciled. Rejected work is `blocked`. The legacy run `status` remains a compatibility projection, so `complete_node` can no longer erase an approval or rejection.

Reconciliation checks use `passed`, `failed`, or `not_evaluated`. Missing prerequisites and grace-period checks are never reported as successful. Review decisions are serialized with a PostgreSQL row lock, persisted once, and safely replayed from the LangGraph checkpoint after a narrow checkpoint/database crash window. This does not change the Redis Streams at-least-once boundary, deterministic job identity, ACK timing, retries, or DLQ behavior described above.

**Doc-type-aware extraction schemas.** Invoices, BOLs, PODs, and rate confirmations have different fields — line items and amounts on invoices, pickup dates on BOLs, delivery condition on PODs. A single universal schema would force nullable fields for every doc type and degrade extraction accuracy. Instead, Claude first classifies the document, then uses a type-specific prompt and Pydantic schema (`InvoiceExtraction`, `BOLExtraction`, `PODExtraction`) to validate structured output. Rate confirmations reuse the invoice schema shape for compatibility with the existing extraction pipeline.
