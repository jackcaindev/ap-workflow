# Freight AP Workflow

Freight AP Workflow automates accounts payable reconciliation for freight carriers. The system ingests invoices, bills of lading (BOLs), and proofs of delivery (PODs) from Gmail, extracts structured data from each document using Claude vision, assembles shipments by load number, and reconciles invoice amounts and supporting paperwork against rate confirmations. When reconciliation fails or data is ambiguous, exceptions are flagged for human review before invoices are approved or rejected.

## Architecture

The backend is a **FastAPI** service that orchestrates document processing through a **LangGraph** state machine with PostgreSQL-backed checkpoints. **PostgreSQL** stores shipments, documents, rate confirmations, reconciliation results, and workflow state. **Redis** decouples Gmail polling from invoice processing via a job queue.

On startup, the API runs two background tasks: a Gmail poller that enqueues attachment jobs every five minutes, and an invoice worker that dequeues jobs, runs the LangGraph workflow (extract → match/reconcile → optional human interrupt), and sends batch summary emails. A **React** UI (Vite + Tailwind) talks to the API for shipment dashboards, reconciliation detail, and human-in-the-loop approval.

```
Gmail Inbox
    │
    ▼
Gmail Poller ──enqueue──▶ Redis (invoice_queue)
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

- Gmail ingestion with Redis job queue
- Claude vision extraction for invoices, BOLs, and PODs
- Automatic shipment assembly by load number
- Full reconciliation: amount variance, carrier match, BOL pickup date, POD confirmation
- Human-in-the-loop review for exceptions via LangGraph interrupt
- Batch email summary notifications
- React UI with shipment dashboard, reconciliation detail, and HITL approval
- Eval suite with 13 passing tests

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

```bash
docker compose exec api pytest tests/ -v
```

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
│   ├── gmail_poller.py      # Inbox polling and Redis enqueue
│   ├── invoice_worker.py    # Redis dequeue and LangGraph invocation
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

**Redis queue decouples polling from processing.** Gmail polling is I/O-bound and runs on a fixed interval; invoice processing is slow (Claude API calls, DB writes, reconciliation). Pushing attachment payloads onto a Redis list (`invoice_queue`) lets the poller finish quickly after enqueueing, while the worker consumes jobs at its own pace. If processing backs up, messages accumulate in Redis rather than blocking the poller or timing out Gmail API calls.

**Partial reconciliation on incomplete document sets.** Freight paperwork arrives asynchronously — an invoice may show up days before the BOL or POD. Rather than waiting for all four documents, reconciliation runs on whatever is present and marks checks as skipped when required data is missing. Shipments get a `partial` status when docs are still outstanding and `exception` only when a present document fails a check (or a POD is overdue past a 3-day grace period). This gives operations useful status early instead of a binary "not ready" state.

**Doc-type-aware extraction schemas.** Invoices, BOLs, PODs, and rate confirmations have different fields — line items and amounts on invoices, pickup dates on BOLs, delivery condition on PODs. A single universal schema would force nullable fields for every doc type and degrade extraction accuracy. Instead, Claude first classifies the document, then uses a type-specific prompt and Pydantic schema (`InvoiceExtraction`, `BOLExtraction`, `PODExtraction`) to validate structured output. Rate confirmations reuse the invoice schema shape for compatibility with the existing extraction pipeline.
