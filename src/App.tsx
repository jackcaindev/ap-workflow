import { useCallback, useEffect, useMemo, useState } from "react";
import {
  checkOutcome,
  normalizeBusinessState,
  type CheckOutcome,
  type PostingStatus,
  type ProcessingStatus,
  type ReviewDisposition,
} from "./businessState";

type View = "dashboard" | "shipment_detail" | "runs" | "detail" | "analytics" | "notifications" | "queue";

type RunStatus =
  | "complete"
  | "awaiting_review"
  | "failed"
  | "running"
  | "pending"
  | "extracted"
  | "matched"
  | "approved"
  | "rejected"
  | string;

type RunSummary = {
  run_id: string;
  filename: string;
  doc_type: string | null;
  carrier_name: string | null;
  amount: number | null;
  status: RunStatus;
  processing_status: ProcessingStatus;
  reconciliation_status: ShipmentStatus | null;
  review_disposition: ReviewDisposition;
  posting_status: PostingStatus;
  reviewed_at: string | null;
  reviewer_id: string | null;
  created_at: string;
  triage_route: TriageRoute | null;
  triage_reasoning: string | null;
  triage_confidence: number | null;
};

type TriageRoute = "auto_resolve" | "escalate_standard" | "escalate_priority";

type LineItem = {
  description: string;
  quantity: number | null;
  unit_price: number | null;
  total: number;
};

type ExtractionData = Record<string, unknown> & {
  doc_type?: string | null;
  invoice_number?: string | null;
  carrier_name?: string | null;
  load_number?: string | null;
  invoice_date?: string | null;
  total_amount?: number | null;
  line_items?: LineItem[];
  bol_number?: string | null;
  pickup_date?: string | null;
  pieces?: number | null;
  weight_lbs?: number | null;
  commodity_description?: string | null;
  delivery_date?: string | null;
  delivery_time?: string | null;
  pieces_received?: number | null;
  condition?: string | null;
  receiver_name?: string | null;
  origin?: string | null;
  destination?: string | null;
  agreed_rate?: number | null;
  shipment_date?: string | null;
};

type MatchResult = {
  matched: boolean;
  reason?: string;
  agreed_rate?: number;
  invoiced_amount?: number;
  variance?: number;
};

type RunDetailData = {
  run_id: string;
  filename: string;
  status: RunStatus;
  processing_status: ProcessingStatus;
  reconciliation_status: ShipmentStatus | null;
  review_disposition: ReviewDisposition;
  posting_status: PostingStatus;
  reviewed_at: string | null;
  reviewer_id: string | null;
  created_at: string;
  updated_at: string;
  extraction: ExtractionData | null;
  match_result: MatchResult | null;
  exception_reason: string | null;
  triage_route: TriageRoute | null;
  triage_reasoning: string | null;
  triage_confidence: number | null;
};

type AuditLogEntry = {
  id: number;
  run_id: string;
  event_type: string;
  payload: Record<string, unknown> | null;
  actor: string | null;
  created_at: string;
};

type BatchNotificationRecord = {
  kind: "batch_summary";
  id: number;
  sent_at: string;
  total_count: number;
  complete_count: number;
  awaiting_review_count: number;
  failed_count: number;
  approved_count: number;
  rejected_count: number;
  ready_for_posting_count: number;
};

type ShipmentExceptionNotificationRecord = {
  kind: "shipment_exception";
  id: string;
  sent_at: string | null;
  occurred_at: string;
  notification_status: string;
  transition: "opened" | "changed" | "resolved";
  shipment_id: string;
  load_number: string;
  missing_docs: string[];
  reason_codes: string[];
};

type NotificationRecord = BatchNotificationRecord | ShipmentExceptionNotificationRecord;

type ReplayState = "enqueued" | "processing" | "retrying" | "acknowledged" | "dead_lettered";

type DLQEntry = {
  dlq_id: string;
  original_stream_id: string | null;
  failure_reason: string | null;
  attempt_count: number;
  failed_at: string | null;
  filename: string | null;
  source: {
    gmail_account: string | null;
    message_id: string | null;
    mime_part_id: string | null;
    idempotency_key: string | null;
  };
  replayable: boolean;
  replay_block_reason: string | null;
  replay: {
    count: number;
    last_request_id: string | null;
    last_requested_at: string | null;
    last_enqueued_at: string | null;
    last_live_stream_id: string | null;
    state: ReplayState | null;
    workflow_processing_status: string | null;
  };
};

type DLQPage = { items: DLQEntry[]; next_cursor: string | null };

type QueueMetrics = {
  live_stream_length: number;
  pending_count: number;
  oldest_pending_age_seconds: number | null;
  dlq_count: number;
  observed_at: string;
};

type PurgeResult = { before: string; purged_count: number; has_more: boolean };

type ShipmentStatus = "pending" | "partial" | "reconciled" | "exception" | string;

type ShipmentSummary = {
  id: string;
  load_number: string;
  carrier_name: string | null;
  reconciliation_status: ShipmentStatus;
  has_invoice: boolean;
  has_rate_con: boolean;
  has_bol: boolean;
  has_pod: boolean;
  created_at: string;
  updated_at: string;
  missing_document_state: "complete" | "within_grace" | "overdue";
  missing_document_deadline_at: string;
  missing_required_docs: string[];
  overdue_reason_codes: string[];
};

type ReconciliationCheck = {
  check_name: string;
  outcome?: CheckOutcome;
  passed?: boolean;
  details: string;
  reason_code?: string | null;
};

type ShipmentDocument = {
  id: number;
  filename: string;
  doc_type: string;
  status: string;
  extracted_data: ExtractionData | null;
  created_at: string;
};

type ShipmentDetailData = ShipmentSummary & {
  documents: {
    invoice: ShipmentDocument | null;
    rate_con: ShipmentDocument | null;
    bol: ShipmentDocument | null;
    pod: ShipmentDocument | null;
  };
  reconciliation_result: {
    id: string;
    run_id: string | null;
    evaluation_source: string;
    evaluation_key: string | null;
    checks: ReconciliationCheck[];
    missing_docs: string[];
    exception_reasons: string[];
    created_at: string;
  } | null;
  missing_document_exception: {
    id: string;
    status: "active" | "resolved";
    missing_docs: string[];
    reason_codes: string[];
    deadline_at: string;
    version: number;
    opened_at: string;
    resolved_at: string | null;
    events: Array<{
      id: string;
      version: number;
      transition: "opened" | "changed" | "resolved";
      before_state: Record<string, unknown> | null;
      after_state: Record<string, unknown>;
      occurred_at: string;
      notification_status: string;
    }>;
  } | null;
};

type CarrierAnalyticsRow = {
  carrier_name: string;
  total_shipments: number;
  exception_count: number;
  exception_rate: number;
  most_common_exception_type: string | null;
  pending_review_count: number;
  approved_count: number;
  rejected_count: number;
  ready_for_posting_count: number;
  partial_within_grace_count: number;
  overdue_missing_documents_count: number;
};

const API_URL = (import.meta.env.VITE_API_URL ?? "http://localhost:8000").replace(/\/$/, "");

const moneyFormatter = new Intl.NumberFormat("en-US", {
  style: "currency",
  currency: "USD",
});

const dateFormatter = new Intl.DateTimeFormat("en-US", {
  dateStyle: "medium",
  timeStyle: "short",
});

async function requestJson<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${API_URL}${path}`, {
    headers: { "Content-Type": "application/json", ...init?.headers },
    ...init,
  });

  if (!response.ok) {
    const message = await response.text();
    throw new Error(message || `Request failed with ${response.status}`);
  }

  return response.json() as Promise<T>;
}

function formatMoney(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "-";
  }
  return moneyFormatter.format(value);
}

function formatDate(value: string | null | undefined): string {
  if (!value) {
    return "-";
  }
  return dateFormatter.format(new Date(value));
}

function formatPercent(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "-";
  }
  return `${(value * 100).toFixed(1)}%`;
}

function labelStatus(status: RunStatus): string {
  return status.replaceAll("_", " ");
}

function parseKeyValueDetails(details: string): Record<string, string> {
  const result: Record<string, string> = {};
  for (const part of details.split(",")) {
    const eqIndex = part.indexOf("=");
    if (eqIndex === -1) {
      continue;
    }
    const key = part.slice(0, eqIndex).trim();
    const value = part.slice(eqIndex + 1).trim();
    result[key] = value;
  }
  return result;
}

function formatReconciliationCheckDetails(check: ReconciliationCheck): string {
  const { check_name, details } = check;
  const outcome = checkOutcome(check);

  if (details.startsWith("skipped:")) {
    return "Skipped — awaiting documents";
  }

  switch (check_name) {
    case "amount_variance": {
      const parsed = parseKeyValueDetails(details);
      const invoice = Number(parsed.invoice);
      const agreedRate = Number(parsed.agreed_rate);
      const variance = Number(parsed.variance);
      const invoiceStr = formatMoney(invoice);
      const agreedStr = formatMoney(agreedRate);
      const varianceStr = formatMoney(variance);
      if (outcome === "passed") {
        return `Invoice ${invoiceStr} matches agreed rate ${agreedStr} (variance ${varianceStr})`;
      }
      return `Invoice ${invoiceStr} vs agreed rate ${agreedStr} — variance ${varianceStr} exceeds 5% tolerance`;
    }
    case "carrier_match": {
      const parsed = parseKeyValueDetails(details);
      return `Invoice carrier: ${parsed.invoice ?? "-"} / Rate con carrier: ${parsed.rate_con ?? "-"}`;
    }
    case "bol_pickup_date": {
      const parsed = parseKeyValueDetails(details);
      return `BOL pickup: ${parsed.bol_pickup_date ?? "-"} / Rate con shipment date: ${parsed.rate_con_shipment_date ?? "-"}`;
    }
    case "pod_delivery_confirmation": {
      if (!details.includes("=")) {
        return details;
      }
      const parsed = parseKeyValueDetails(details);
      const deliveryDate = parsed.delivery_date === "missing" ? "-" : (parsed.delivery_date ?? "-");
      const condition = parsed.condition === "missing" ? "-" : (parsed.condition ?? "-");
      return `Delivered ${deliveryDate}, condition: ${condition}`;
    }
    case "missing_docs": {
      if (details.toLowerCase() === "none" || details === "") {
        return "None";
      }
      return details
        .split(",")
        .map((doc) => labelStatus(doc.trim()))
        .join(", ");
    }
    default:
      return details;
  }
}

function TriageBadge({ route }: { route: TriageRoute }) {
  const className = useMemo(() => {
    if (route === "auto_resolve") {
      return "border-slate-200 bg-slate-100 text-slate-600";
    }
    if (route === "escalate_priority") {
      return "border-orange-200 bg-orange-50 text-orange-700";
    }
    return "border-slate-200 bg-slate-50 text-slate-700";
  }, [route]);

  return (
    <span className={`inline-flex rounded-full border px-2.5 py-1 text-xs font-medium ${className}`}>
      {labelStatus(route)}
    </span>
  );
}

function StatusBadge({ status }: { status: RunStatus }) {
  const className = useMemo(() => {
    if (status === "complete" || status === "approved" || status === "reconciled") {
      return "border-emerald-200 bg-emerald-50 text-emerald-700";
    }
    if (status === "awaiting_review" || status === "running" || status === "pending" || status === "partial") {
      return "border-amber-200 bg-amber-50 text-amber-700";
    }
    if (status === "failed" || status === "rejected" || status === "exception") {
      return "border-red-200 bg-red-50 text-red-700";
    }
    return "border-slate-200 bg-slate-50 text-slate-700";
  }, [status]);

  return (
    <span className={`inline-flex rounded-full border px-2.5 py-1 text-xs font-medium ${className}`}>
      {labelStatus(status)}
    </span>
  );
}

function MissingDocumentBadge({ state }: { state: ShipmentSummary["missing_document_state"] }) {
  const label = state === "within_grace" ? "Partial — within grace" : state === "overdue" ? "Overdue documents" : "Documents complete";
  const className = state === "overdue"
    ? "border-red-200 bg-red-50 text-red-700"
    : state === "within_grace"
      ? "border-amber-200 bg-amber-50 text-amber-700"
      : "border-emerald-200 bg-emerald-50 text-emerald-700";
  return <span className={`inline-flex rounded-full border px-2.5 py-1 text-xs font-medium ${className}`}>{label}</span>;
}

function Sidebar({
  activeView,
  hasSelectedRun,
  hasSelectedShipment,
  onSelect,
}: {
  activeView: View;
  hasSelectedRun: boolean;
  hasSelectedShipment: boolean;
  onSelect: (view: View) => void;
}) {
  const items: { view: View; label: string; disabled?: boolean }[] = [
    { view: "dashboard", label: "Dashboard" },
    { view: "shipment_detail", label: "Shipment Detail", disabled: !hasSelectedShipment },
    { view: "runs", label: "Runs" },
    { view: "detail", label: "Run Detail", disabled: !hasSelectedRun },
    { view: "analytics", label: "Carrier Analytics" },
    { view: "notifications", label: "Notifications" },
    { view: "queue", label: "Queue Operations" },
  ];

  return (
    <aside className="flex min-h-screen w-64 shrink-0 flex-col bg-slate-950 px-4 py-5 text-white">
      <div className="mb-8 px-2">
        <div className="text-sm font-semibold uppercase tracking-wide text-slate-400">Freight AP</div>
        <div className="mt-2 text-xl font-semibold">Workflow Ops</div>
      </div>
      <nav className="space-y-1">
        {items.map((item) => (
          <button
            key={item.view}
            type="button"
            disabled={item.disabled}
            onClick={() => onSelect(item.view)}
            className={`w-full rounded-md px-3 py-2 text-left text-sm font-medium transition ${
              activeView === item.view
                ? "bg-white text-slate-950"
                : "text-slate-300 hover:bg-slate-900 hover:text-white"
            } ${item.disabled ? "cursor-not-allowed opacity-40 hover:bg-transparent hover:text-slate-300" : ""}`}
          >
            {item.label}
          </button>
        ))}
      </nav>
      <div className="mt-auto rounded-md border border-slate-800 px-3 py-3 text-xs text-slate-400">
        API: {API_URL}
      </div>
    </aside>
  );
}

function Runs({ onOpenRun }: { onOpenRun: (runId: string) => void }) {
  const [runs, setRuns] = useState<RunSummary[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const loadRuns = useCallback(async () => {
    try {
      setError(null);
      const data = await requestJson<RunSummary[]>("/workflow/runs");
      setRuns(data.map(normalizeBusinessState));
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load workflow runs");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadRuns();
    const intervalId = window.setInterval(() => {
      void loadRuns();
    }, 10_000);
    return () => window.clearInterval(intervalId);
  }, [loadRuns]);

  return (
    <section>
      <div className="mb-6 flex flex-wrap items-center justify-between gap-3">
        <div>
          <h1 className="text-2xl font-semibold text-slate-950">Runs</h1>
          <p className="mt-1 text-sm text-slate-500">
            Review queue and recent workflow runs. Priority triage items appear first.
          </p>
        </div>
      </div>

      <div className="overflow-hidden rounded-lg border border-slate-200 bg-white">
        <table className="w-full border-collapse text-left text-sm">
          <thead className="bg-slate-50 text-xs uppercase text-slate-500">
            <tr>
              <th className="px-4 py-3 font-semibold">Filename</th>
              <th className="px-4 py-3 font-semibold">Carrier</th>
              <th className="px-4 py-3 font-semibold">Amount</th>
              <th className="px-4 py-3 font-semibold">Processing</th>
              <th className="px-4 py-3 font-semibold">Reconciliation</th>
              <th className="px-4 py-3 font-semibold">Review</th>
              <th className="px-4 py-3 font-semibold">Downstream</th>
              <th className="px-4 py-3 font-semibold">Triage</th>
              <th className="px-4 py-3 font-semibold">Created</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-100">
            {runs.map((run) => (
              <tr
                key={run.run_id}
                onClick={() => onOpenRun(run.run_id)}
                className="cursor-pointer hover:bg-slate-50"
              >
                <td className="px-4 py-3 font-medium text-slate-900">{run.filename}</td>
                <td className="px-4 py-3 text-slate-600">{run.carrier_name ?? "-"}</td>
                <td className="px-4 py-3 text-slate-600">
                  {run.doc_type === "invoice" ? formatMoney(run.amount) : "-"}
                </td>
                <td className="px-4 py-3">
                  <StatusBadge status={run.processing_status} />
                </td>
                <td className="px-4 py-3">
                  {run.reconciliation_status ? <StatusBadge status={run.reconciliation_status} /> : "-"}
                </td>
                <td className="px-4 py-3">
                  <StatusBadge status={run.review_disposition} />
                </td>
                <td className="px-4 py-3">
                  <StatusBadge status={run.posting_status} />
                </td>
                <td className="px-4 py-3">
                  {run.triage_route ? <TriageBadge route={run.triage_route} /> : "-"}
                </td>
                <td className="px-4 py-3 text-slate-600">{formatDate(run.created_at)}</td>
              </tr>
            ))}
            {!loading && runs.length === 0 ? (
              <tr>
                <td className="px-4 py-8 text-center text-slate-500" colSpan={9}>
                  No workflow runs yet.
                </td>
              </tr>
            ) : null}
            {loading ? (
              <tr>
                <td className="px-4 py-8 text-center text-slate-500" colSpan={9}>
                  Loading workflow runs...
                </td>
              </tr>
            ) : null}
          </tbody>
        </table>
      </div>
      {error ? <div className="mt-3 rounded-md bg-red-50 px-3 py-2 text-sm text-red-700">{error}</div> : null}
    </section>
  );
}

function Dashboard({ onOpenShipment }: { onOpenShipment: (shipmentId: string) => void }) {
  const [shipments, setShipments] = useState<ShipmentSummary[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [polling, setPolling] = useState(false);
  const [pollMessage, setPollMessage] = useState<string | null>(null);

  const loadShipments = useCallback(async () => {
    try {
      setError(null);
      const data = await requestJson<ShipmentSummary[]>("/shipments");
      setShipments(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load shipments");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadShipments();
    const intervalId = window.setInterval(() => {
      void loadShipments();
    }, 10_000);
    return () => window.clearInterval(intervalId);
  }, [loadShipments]);

  async function handlePollGmail() {
    setPolling(true);
    setPollMessage(null);
    try {
      const result = await requestJson<{ count: number }>("/gmail/poll", { method: "POST" });
      setPollMessage(`${result.count} new message${result.count === 1 ? "" : "s"} found`);
      await loadShipments();
    } catch (err) {
      setPollMessage(err instanceof Error ? err.message : "Gmail polling failed");
    } finally {
      setPolling(false);
    }
  }

  return (
    <section>
      <div className="mb-6 flex flex-wrap items-center justify-between gap-3">
        <div>
          <h1 className="text-2xl font-semibold text-slate-950">Dashboard</h1>
          <p className="mt-1 text-sm text-slate-500">Shipment reconciliation status by load.</p>
        </div>
        <div className="flex items-center gap-3">
          {pollMessage ? <span className="text-sm text-slate-600">{pollMessage}</span> : null}
          <button
            type="button"
            onClick={handlePollGmail}
            disabled={polling}
            className="rounded-md bg-slate-950 px-4 py-2 text-sm font-medium text-white hover:bg-slate-800 disabled:cursor-not-allowed disabled:bg-slate-400"
          >
            {polling ? "Polling..." : "Poll Gmail"}
          </button>
        </div>
      </div>

      <div className="overflow-hidden rounded-lg border border-slate-200 bg-white">
        <table className="w-full border-collapse text-left text-sm">
          <thead className="bg-slate-50 text-xs uppercase text-slate-500">
            <tr>
              <th className="px-4 py-3 font-semibold">Load Number</th>
              <th className="px-4 py-3 font-semibold">Carrier</th>
              <th className="px-4 py-3 font-semibold">Status</th>
              <th className="px-4 py-3 font-semibold">Documents</th>
              <th className="px-4 py-3 font-semibold">Created</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-100">
            {shipments.map((shipment) => (
              <tr
                key={shipment.id}
                onClick={() => onOpenShipment(shipment.id)}
                className="cursor-pointer hover:bg-slate-50"
              >
                <td className="px-4 py-3 font-medium text-slate-900">{shipment.load_number}</td>
                <td className="px-4 py-3 text-slate-600">{shipment.carrier_name ?? "-"}</td>
                <td className="px-4 py-3">
                  <div className="flex flex-col items-start gap-1">
                    <StatusBadge status={shipment.reconciliation_status} />
                    <MissingDocumentBadge state={shipment.missing_document_state} />
                  </div>
                </td>
                <td className="px-4 py-3">
                  <div className="flex flex-wrap gap-2">
                    <DocIndicator present={shipment.has_invoice} label="INVOICE" />
                    <DocIndicator present={shipment.has_rate_con} label="RATE CON" />
                    <DocIndicator present={shipment.has_bol} label="BOL" />
                    <DocIndicator present={shipment.has_pod} label="POD" />
                  </div>
                </td>
                <td className="px-4 py-3 text-slate-600">{formatDate(shipment.created_at)}</td>
              </tr>
            ))}
            {!loading && shipments.length === 0 ? (
              <tr>
                <td className="px-4 py-8 text-center text-slate-500" colSpan={6}>
                  No shipments found.
                </td>
              </tr>
            ) : null}
            {loading ? (
              <tr>
                <td className="px-4 py-8 text-center text-slate-500" colSpan={6}>
                  Loading shipments...
                </td>
              </tr>
            ) : null}
          </tbody>
        </table>
      </div>
      {error ? <div className="mt-3 rounded-md bg-red-50 px-3 py-2 text-sm text-red-700">{error}</div> : null}
    </section>
  );
}

function Field({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <dt className="text-xs font-medium uppercase text-slate-500">{label}</dt>
      <dd className="mt-1 text-sm font-medium text-slate-950">{value}</dd>
    </div>
  );
}

const extractionFieldLabels: Record<string, string> = {
  invoice_number: "Invoice Number",
  carrier_name: "Carrier",
  load_number: "Load Number",
  invoice_date: "Invoice Date",
  total_amount: "Total Amount",
  bol_number: "BOL Number",
  pickup_date: "Pickup Date",
  pieces: "Pieces",
  weight_lbs: "Weight Lbs",
  commodity_description: "Commodity",
  delivery_date: "Delivery Date",
  delivery_time: "Delivery Time",
  pieces_received: "Pieces Received",
  condition: "Condition",
  receiver_name: "Receiver",
  origin: "Origin",
  destination: "Destination",
  agreed_rate: "Agreed Rate",
  shipment_date: "Shipment Date",
};

const extractionFieldsByDocType: Record<string, string[]> = {
  invoice: ["invoice_number", "carrier_name", "load_number", "invoice_date", "total_amount"],
  bill_of_lading: [
    "bol_number",
    "carrier_name",
    "load_number",
    "pickup_date",
    "pieces",
    "weight_lbs",
    "commodity_description",
  ],
  proof_of_delivery: [
    "bol_number",
    "carrier_name",
    "load_number",
    "delivery_date",
    "delivery_time",
    "pieces_received",
    "condition",
    "receiver_name",
  ],
  rate_confirmation: [
    "load_number",
    "carrier_name",
    "origin",
    "destination",
    "agreed_rate",
    "shipment_date",
  ],
};

function fieldLabel(key: string): string {
  return extractionFieldLabels[key] ?? labelStatus(key);
}

function formatExtractionValue(key: string, value: unknown): string {
  if (value === null || value === undefined || value === "") {
    return "-";
  }
  if (key === "total_amount" || key === "agreed_rate") {
    return typeof value === "number" ? formatMoney(value) : String(value);
  }
  if (Array.isArray(value) || typeof value === "object") {
    return JSON.stringify(value);
  }
  return String(value);
}

function GenericExtractionTable({ extraction }: { extraction: ExtractionData }) {
  return (
    <div className="overflow-hidden rounded-lg border border-slate-200 bg-white">
      <div className="border-b border-slate-200 px-5 py-4">
        <h2 className="text-base font-semibold text-slate-950">Extraction</h2>
      </div>
      <table className="w-full border-collapse text-left text-sm">
        <tbody className="divide-y divide-slate-100">
          {Object.entries(extraction).map(([key, value]) => (
            <tr key={key}>
              <th className="w-64 bg-slate-50 px-4 py-3 text-xs font-semibold uppercase text-slate-500">
                {fieldLabel(key)}
              </th>
              <td className="px-4 py-3 text-slate-700">{formatExtractionValue(key, value)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function ExtractionSection({ extraction, createdAt }: { extraction: ExtractionData | null; createdAt: string }) {
  if (extraction === null) {
    return (
      <div className="rounded-lg border border-slate-200 bg-white p-5">
        <h2 className="mb-2 text-base font-semibold text-slate-950">Extraction</h2>
        <p className="text-sm text-slate-500">No extraction data available.</p>
      </div>
    );
  }

  const docType = typeof extraction.doc_type === "string" ? extraction.doc_type : "unknown";
  const fields = extractionFieldsByDocType[docType];

  if (fields === undefined) {
    return <GenericExtractionTable extraction={extraction} />;
  }

  return (
    <div className="rounded-lg border border-slate-200 bg-white p-5">
      <div className="mb-4 flex items-center justify-between gap-3">
        <h2 className="text-base font-semibold text-slate-950">Extraction</h2>
        <span className="rounded-full border border-slate-200 bg-slate-50 px-2.5 py-1 text-xs font-medium text-slate-700">
          {labelStatus(docType)}
        </span>
      </div>
      <dl className="grid gap-4 md:grid-cols-3">
        {fields.map((key) => (
          <Field key={key} label={fieldLabel(key)} value={formatExtractionValue(key, extraction[key])} />
        ))}
        <Field label="Created" value={formatDate(createdAt)} />
      </dl>
    </div>
  );
}

function formatAuditPayloadSummary(entry: AuditLogEntry): string {
  const payload = entry.payload;
  if (!payload) {
    return "-";
  }

  switch (entry.event_type) {
    case "extracted": {
      const extraction = payload.extraction;
      if (!extraction || typeof extraction !== "object") {
        return "-";
      }
      const summary = extraction as Record<string, unknown>;
      const parts = [
        summary.doc_type ? labelStatus(String(summary.doc_type)) : null,
        summary.carrier_name ? String(summary.carrier_name) : null,
        summary.load_number ? `Load ${String(summary.load_number)}` : null,
      ].filter(Boolean);
      return parts.length > 0 ? parts.join(" · ") : "-";
    }
    case "exception_raised":
      return typeof payload.exception_reason === "string" ? payload.exception_reason : "-";
    case "triaged": {
      const route = typeof payload.route === "string" ? labelStatus(payload.route) : null;
      const reasoning = typeof payload.reasoning === "string" ? payload.reasoning : null;
      if (route && reasoning) {
        return `${route}: ${reasoning}`;
      }
      return route ?? reasoning ?? "-";
    }
    case "approved":
    case "rejected":
      return typeof payload.human_decision === "string"
        ? labelStatus(payload.human_decision)
        : labelStatus(entry.event_type);
    case "completed":
      return "Workflow finished";
    default:
      return JSON.stringify(payload);
  }
}

function AuditTimeline({ runId }: { runId: string }) {
  const [entries, setEntries] = useState<AuditLogEntry[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const loadAudit = useCallback(async () => {
    try {
      setError(null);
      const data = await requestJson<AuditLogEntry[]>(`/workflow/${runId}/audit`);
      setEntries(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load audit trail");
    } finally {
      setLoading(false);
    }
  }, [runId]);

  useEffect(() => {
    setLoading(true);
    void loadAudit();
  }, [loadAudit]);

  return (
    <div className="rounded-lg border border-slate-200 bg-white p-5">
      <h2 className="mb-4 text-base font-semibold text-slate-950">Audit Trail</h2>
      {loading ? <p className="text-sm text-slate-500">Loading audit trail...</p> : null}
      {error ? <div className="mb-3 rounded-md bg-red-50 px-3 py-2 text-sm text-red-700">{error}</div> : null}
      {!loading && entries.length === 0 ? (
        <p className="text-sm text-slate-500">No audit events yet.</p>
      ) : null}
      {entries.length > 0 ? (
        <ol className="relative border-l border-slate-200 pl-5">
          {entries.map((entry) => (
            <li key={entry.id} className="mb-6 last:mb-0">
              <span className="absolute -left-1.5 mt-1.5 h-3 w-3 rounded-full border-2 border-white bg-slate-400" />
              <div className="flex flex-wrap items-center gap-2">
                <span className="text-sm font-medium text-slate-900">{labelStatus(entry.event_type)}</span>
                <span className="text-xs text-slate-500">{formatDate(entry.created_at)}</span>
                {entry.actor ? (
                  <span className="text-xs text-slate-500">by {entry.actor}</span>
                ) : null}
              </div>
              <p className="mt-1 text-sm text-slate-600">{formatAuditPayloadSummary(entry)}</p>
            </li>
          ))}
        </ol>
      ) : null}
    </div>
  );
}

function RunDetail({
  runId,
  onBack,
  onViewShipment,
}: {
  runId: string;
  onBack: () => void;
  onViewShipment: (loadNumber: string) => void;
}) {
  const [detail, setDetail] = useState<RunDetailData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [deciding, setDeciding] = useState<"approved" | "rejected" | null>(null);

  const loadDetail = useCallback(async () => {
    try {
      setError(null);
      const data = await requestJson<RunDetailData>(`/workflow/${runId}`);
      setDetail(normalizeBusinessState(data));
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load workflow run");
    } finally {
      setLoading(false);
    }
  }, [runId]);

  useEffect(() => {
    setLoading(true);
    void loadDetail();
  }, [loadDetail]);

  async function decide(decision: "approved" | "rejected") {
    setDeciding(decision);
    try {
      await requestJson<{ run_id: string; status: RunStatus }>(`/workflow/${runId}/resume`, {
        method: "POST",
        body: JSON.stringify({ decision }),
      });
      await loadDetail();
    } catch (err) {
      const message = err instanceof Error ? err.message : "Decision failed";
      await loadDetail();
      setError(message);
    } finally {
      setDeciding(null);
    }
  }

  const extraction = detail?.extraction;
  const docType = typeof extraction?.doc_type === "string" ? extraction.doc_type : "unknown";
  const isInvoice = docType === "invoice";
  const match = isInvoice ? detail?.match_result : null;
  const lineItems = isInvoice && Array.isArray(extraction?.line_items) ? extraction.line_items : [];
  const loadNumber =
    typeof extraction?.load_number === "string" && extraction.load_number.trim() !== ""
      ? extraction.load_number
      : null;

  return (
    <section>
      <div className="mb-6 flex flex-wrap items-center justify-between gap-3">
        <div>
          <div className="mb-2 flex flex-wrap items-center gap-4">
            <button type="button" onClick={onBack} className="text-sm font-medium text-slate-600 hover:text-slate-950">
              Back to Runs
            </button>
            {loadNumber ? (
              <button
                type="button"
                onClick={() => onViewShipment(loadNumber)}
                className="text-sm font-medium text-slate-600 hover:text-slate-950"
              >
                View Shipment
              </button>
            ) : null}
          </div>
          <h1 className="text-2xl font-semibold text-slate-950">Run Detail</h1>
          <p className="mt-1 text-sm text-slate-500">{detail?.filename ?? runId}</p>
        </div>
        {detail ? <StatusBadge status={detail.review_disposition} /> : null}
      </div>

      {error ? <div className="mb-4 rounded-md bg-red-50 px-3 py-2 text-sm text-red-700">{error}</div> : null}
      {loading ? <div className="text-sm text-slate-500">Loading workflow run...</div> : null}

      {detail ? (
        <div className="space-y-6">
          <div className="rounded-lg border border-slate-200 bg-white p-5">
            <h2 className="mb-4 text-base font-semibold text-slate-950">Business State</h2>
            <div className="grid gap-4 md:grid-cols-4">
              <Field label="Processing" value={labelStatus(detail.processing_status)} />
              <Field
                label="Reconciliation"
                value={detail.reconciliation_status ? labelStatus(detail.reconciliation_status) : "-"}
              />
              <Field label="Review" value={labelStatus(detail.review_disposition)} />
              <Field label="Downstream" value={labelStatus(detail.posting_status)} />
              <Field label="Decision time" value={formatDate(detail.reviewed_at)} />
              <Field label="Reviewer" value={detail.reviewer_id ?? "Reviewer not captured"} />
            </div>
          </div>

          <ExtractionSection extraction={extraction ?? null} createdAt={detail.created_at} />

          {isInvoice ? (
            <div className="overflow-hidden rounded-lg border border-slate-200 bg-white">
              <div className="border-b border-slate-200 px-5 py-4">
                <h2 className="text-base font-semibold text-slate-950">Line Items</h2>
              </div>
              <table className="w-full border-collapse text-left text-sm">
                <thead className="bg-slate-50 text-xs uppercase text-slate-500">
                  <tr>
                    <th className="px-4 py-3 font-semibold">Description</th>
                    <th className="px-4 py-3 font-semibold">Quantity</th>
                    <th className="px-4 py-3 font-semibold">Unit Price</th>
                    <th className="px-4 py-3 font-semibold">Total</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-100">
                  {lineItems.map((item, index) => (
                    <tr key={`${item.description}-${index}`}>
                      <td className="px-4 py-3 font-medium text-slate-900">{item.description}</td>
                      <td className="px-4 py-3 text-slate-600">{item.quantity ?? "-"}</td>
                      <td className="px-4 py-3 text-slate-600">{formatMoney(item.unit_price)}</td>
                      <td className="px-4 py-3 text-slate-600">{formatMoney(item.total)}</td>
                    </tr>
                  ))}
                  {lineItems.length === 0 ? (
                    <tr>
                      <td className="px-4 py-8 text-center text-slate-500" colSpan={4}>
                        No line items available.
                      </td>
                    </tr>
                  ) : null}
                </tbody>
              </table>
            </div>
          ) : null}

          {isInvoice ? (
            <div className="rounded-lg border border-slate-200 bg-white p-5">
              <div className="mb-4 flex items-center justify-between gap-3">
                <h2 className="text-base font-semibold text-slate-950">Match Result</h2>
                {match ? (
                  <span
                    className={`rounded-full border px-2.5 py-1 text-xs font-medium ${
                      match.matched
                        ? "border-emerald-200 bg-emerald-50 text-emerald-700"
                        : "border-amber-200 bg-amber-50 text-amber-700"
                    }`}
                  >
                    {match.matched ? "matched" : "unmatched"}
                  </span>
                ) : null}
              </div>
              <dl className="grid gap-4 md:grid-cols-3">
                <Field label="Agreed Rate" value={formatMoney(match?.agreed_rate)} />
                <Field label="Variance" value={formatMoney(match?.variance)} />
                <Field label="Reason" value={match?.reason ?? detail.exception_reason ?? "-"} />
              </dl>
            </div>
          ) : null}

          {detail.exception_reason ? (
            <div className="rounded-lg border border-amber-200 bg-amber-50 p-5 text-sm text-amber-800">
              <span className="font-semibold">Exception reason:</span> {detail.exception_reason}
            </div>
          ) : null}

          {detail.triage_reasoning ? (
            <div className="rounded-lg border border-slate-200 bg-slate-50 p-5 text-sm text-slate-800">
              <div className="mb-2 flex flex-wrap items-center gap-2">
                <span className="font-semibold">Triage reasoning</span>
                {detail.triage_route ? <TriageBadge route={detail.triage_route} /> : null}
                {detail.triage_confidence !== null && detail.triage_confidence !== undefined ? (
                  <span className="text-xs text-slate-500">
                    {(detail.triage_confidence * 100).toFixed(0)}% confidence
                  </span>
                ) : null}
              </div>
              <p>{detail.triage_reasoning}</p>
            </div>
          ) : null}

          {detail.processing_status === "awaiting_review" && detail.review_disposition === "pending" ? (
            <div className="flex gap-3">
              <button
                type="button"
                onClick={() => void decide("approved")}
                disabled={deciding !== null}
                className="rounded-md bg-emerald-600 px-4 py-2 text-sm font-medium text-white hover:bg-emerald-700 disabled:cursor-not-allowed disabled:bg-slate-400"
              >
                {deciding === "approved" ? "Approving..." : "Approve"}
              </button>
              <button
                type="button"
                onClick={() => void decide("rejected")}
                disabled={deciding !== null}
                className="rounded-md bg-red-600 px-4 py-2 text-sm font-medium text-white hover:bg-red-700 disabled:cursor-not-allowed disabled:bg-slate-400"
              >
                {deciding === "rejected" ? "Rejecting..." : "Reject"}
              </button>
            </div>
          ) : null}

          <AuditTimeline key={detail.updated_at} runId={runId} />
        </div>
      ) : null}
    </section>
  );
}

function DocIndicator({ present, label }: { present: boolean; label: string }) {
  return (
    <span className="inline-flex min-w-14 items-center gap-1 text-sm text-slate-700">
      <span aria-hidden="true">{present ? "✅" : "⚪"}</span>
      <span className="text-xs font-medium uppercase text-slate-500">{label}</span>
    </span>
  );
}

function ShipmentDetail({ shipmentId, onBack }: { shipmentId: string; onBack: () => void }) {
  const [detail, setDetail] = useState<ShipmentDetailData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const loadDetail = useCallback(async () => {
    try {
      setError(null);
      const data = await requestJson<ShipmentDetailData>(`/shipments/${shipmentId}`);
      setDetail(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load shipment");
    } finally {
      setLoading(false);
    }
  }, [shipmentId]);

  useEffect(() => {
    setLoading(true);
    void loadDetail();
  }, [loadDetail]);

  const reconciliation = detail?.reconciliation_result;
  const documents = detail?.documents;

  return (
    <section>
      <div className="mb-6 flex flex-wrap items-center justify-between gap-3">
        <div>
          <button type="button" onClick={onBack} className="mb-2 text-sm font-medium text-slate-600 hover:text-slate-950">
            Back to Dashboard
          </button>
          <h1 className="text-2xl font-semibold text-slate-950">Shipment Detail</h1>
          <p className="mt-1 text-sm text-slate-500">{detail?.load_number ?? shipmentId}</p>
        </div>
        {detail ? (
          <div className="flex flex-wrap gap-2">
            <StatusBadge status={detail.reconciliation_status} />
            <MissingDocumentBadge state={detail.missing_document_state} />
          </div>
        ) : null}
      </div>

      {error ? <div className="mb-4 rounded-md bg-red-50 px-3 py-2 text-sm text-red-700">{error}</div> : null}
      {loading ? <div className="text-sm text-slate-500">Loading shipment...</div> : null}

      {detail ? (
        <div className="space-y-6">
          <div className="rounded-lg border border-slate-200 bg-white p-5">
            <h2 className="mb-4 text-base font-semibold text-slate-950">Shipment</h2>
            <dl className="grid gap-4 md:grid-cols-4">
              <Field label="Load Number" value={detail.load_number} />
              <Field label="Carrier" value={detail.carrier_name ?? "-"} />
              <Field label="Created" value={formatDate(detail.created_at)} />
              <Field label="Updated" value={formatDate(detail.updated_at)} />
            </dl>
            <div className="mt-4 flex flex-wrap gap-3">
              <DocIndicator present={detail.has_invoice} label="Invoice" />
              <DocIndicator present={detail.has_rate_con} label="Rate Con" />
              <DocIndicator present={detail.has_bol} label="BOL" />
              <DocIndicator present={detail.has_pod} label="POD" />
            </div>
          </div>

          <div className="overflow-hidden rounded-lg border border-slate-200 bg-white">
            <div className="border-b border-slate-200 px-5 py-4">
              <h2 className="text-base font-semibold text-slate-950">Reconciliation Checks</h2>
            </div>
            <table className="w-full border-collapse text-left text-sm">
              <thead className="bg-slate-50 text-xs uppercase text-slate-500">
                <tr>
                  <th className="px-4 py-3 font-semibold">Check</th>
                  <th className="px-4 py-3 font-semibold">Result</th>
                  <th className="px-4 py-3 font-semibold">Details</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-100">
                {(reconciliation?.checks ?? []).map((check) => {
                  const outcome = checkOutcome(check);
                  const outcomeClass =
                    outcome === "passed"
                      ? "border-emerald-200 bg-emerald-50 text-emerald-700"
                      : outcome === "failed"
                        ? "border-red-200 bg-red-50 text-red-700"
                        : "border-amber-200 bg-amber-50 text-amber-700";
                  return (
                    <tr key={check.check_name}>
                      <td className="px-4 py-3 font-medium text-slate-900">{labelStatus(check.check_name)}</td>
                      <td className="px-4 py-3">
                        <span className={`rounded-full border px-2.5 py-1 text-xs font-medium ${outcomeClass}`}>
                          {labelStatus(outcome)}
                        </span>
                      </td>
                      <td className="px-4 py-3 text-slate-600">{formatReconciliationCheckDetails(check)}</td>
                    </tr>
                  );
                })}
                {reconciliation === null ? (
                  <tr>
                    <td className="px-4 py-8 text-center text-slate-500" colSpan={3}>
                      No reconciliation result yet.
                    </td>
                  </tr>
                ) : null}
              </tbody>
            </table>
          </div>

          <div className="grid gap-6 lg:grid-cols-2">
            <div className="rounded-lg border border-slate-200 bg-white p-5">
              <h2 className="mb-3 text-base font-semibold text-slate-950">Missing Docs</h2>
              <p className="text-sm text-slate-700">
                {(reconciliation?.missing_docs ?? []).length > 0
                  ? reconciliation?.missing_docs.map(labelStatus).join(", ")
                  : "None"}
              </p>
            </div>
            <div className="rounded-lg border border-slate-200 bg-white p-5">
              <h2 className="mb-3 text-base font-semibold text-slate-950">Exception Reasons</h2>
              <p className="text-sm text-slate-700">
                {(reconciliation?.exception_reasons ?? []).length > 0
                  ? reconciliation?.exception_reasons.map(labelStatus).join(", ")
                  : "None"}
              </p>
            </div>
          </div>

          <div className="rounded-lg border border-slate-200 bg-white p-5">
            <h2 className="mb-3 text-base font-semibold text-slate-950">Missing-document SLA</h2>
            <dl className="grid gap-4 md:grid-cols-3">
              <Field label="State" value={labelStatus(detail.missing_document_state)} />
              <Field label="Deadline" value={formatDate(detail.missing_document_deadline_at)} />
              <Field
                label="Overdue reasons"
                value={detail.overdue_reason_codes.length > 0 ? detail.overdue_reason_codes.map(labelStatus).join(", ") : "None"}
              />
            </dl>
            {(detail.missing_document_exception?.events ?? []).length > 0 ? (
              <div className="mt-4 border-t border-slate-100 pt-4">
                <div className="mb-2 text-xs font-semibold uppercase text-slate-500">Transition history</div>
                <ul className="space-y-2 text-sm text-slate-700">
                  {detail.missing_document_exception?.events.map((event) => (
                    <li key={event.id}>
                      {formatDate(event.occurred_at)} — {labelStatus(event.transition)} (notification {labelStatus(event.notification_status)})
                    </li>
                  ))}
                </ul>
              </div>
            ) : null}
          </div>

          <div className="overflow-hidden rounded-lg border border-slate-200 bg-white">
            <div className="border-b border-slate-200 px-5 py-4">
              <h2 className="text-base font-semibold text-slate-950">Documents</h2>
            </div>
            <table className="w-full border-collapse text-left text-sm">
              <thead className="bg-slate-50 text-xs uppercase text-slate-500">
                <tr>
                  <th className="px-4 py-3 font-semibold">Type</th>
                  <th className="px-4 py-3 font-semibold">Filename</th>
                  <th className="px-4 py-3 font-semibold">Carrier</th>
                  <th className="px-4 py-3 font-semibold">Amount</th>
                  <th className="px-4 py-3 font-semibold">Created</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-100">
                {(["invoice", "rate_con", "bol", "pod"] as const).map((key) => {
                  const document = documents?.[key] ?? null;
                  return (
                    <tr key={key}>
                      <td className="px-4 py-3 font-medium text-slate-900">{labelStatus(key)}</td>
                      <td className="px-4 py-3 text-slate-600">{document?.filename ?? "-"}</td>
                      <td className="px-4 py-3 text-slate-600">{document?.extracted_data?.carrier_name ?? "-"}</td>
                      <td className="px-4 py-3 text-slate-600">
                        {formatMoney(
                          document?.extracted_data?.total_amount ??
                            document?.extracted_data?.agreed_rate,
                        )}
                      </td>
                      <td className="px-4 py-3 text-slate-600">{formatDate(document?.created_at)}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      ) : null}
    </section>
  );
}

function CarrierAnalytics() {
  const [rows, setRows] = useState<CarrierAnalyticsRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const loadAnalytics = useCallback(async () => {
    try {
      setError(null);
      const data = await requestJson<CarrierAnalyticsRow[]>("/analytics/carriers");
      setRows(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load carrier analytics");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadAnalytics();
  }, [loadAnalytics]);

  return (
    <section>
      <div className="mb-6 flex flex-wrap items-center justify-between gap-3">
        <div>
          <h1 className="text-2xl font-semibold text-slate-950">Carrier Analytics</h1>
          <p className="mt-1 text-sm text-slate-500">Exception rate by carrier.</p>
        </div>
        <button
          type="button"
          onClick={() => void loadAnalytics()}
          className="rounded-md border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-50"
        >
          Refresh
        </button>
      </div>

      <div className="overflow-hidden rounded-lg border border-slate-200 bg-white">
        <table className="w-full border-collapse text-left text-sm">
          <thead className="bg-slate-50 text-xs uppercase text-slate-500">
            <tr>
              <th className="px-4 py-3 font-semibold">Carrier</th>
              <th className="px-4 py-3 font-semibold">Shipments</th>
              <th className="px-4 py-3 font-semibold">Exceptions</th>
              <th className="px-4 py-3 font-semibold">Within Grace</th>
              <th className="px-4 py-3 font-semibold">Overdue Docs</th>
              <th className="px-4 py-3 font-semibold">Exception Rate</th>
              <th className="px-4 py-3 font-semibold">Pending Review</th>
              <th className="px-4 py-3 font-semibold">Approved</th>
              <th className="px-4 py-3 font-semibold">Rejected</th>
              <th className="px-4 py-3 font-semibold">Ready to Post</th>
              <th className="px-4 py-3 font-semibold">Most Common Exception</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-100">
            {rows.map((row) => (
              <tr key={row.carrier_name}>
                <td className="px-4 py-3 font-medium text-slate-900">{row.carrier_name}</td>
                <td className="px-4 py-3 text-slate-600">{row.total_shipments}</td>
                <td className="px-4 py-3 text-slate-600">{row.exception_count}</td>
                <td className="px-4 py-3 text-slate-600">{row.partial_within_grace_count}</td>
                <td className="px-4 py-3 text-slate-600">{row.overdue_missing_documents_count}</td>
                <td className="px-4 py-3 text-slate-600">{formatPercent(row.exception_rate)}</td>
                <td className="px-4 py-3 text-slate-600">{row.pending_review_count}</td>
                <td className="px-4 py-3 text-slate-600">{row.approved_count}</td>
                <td className="px-4 py-3 text-slate-600">{row.rejected_count}</td>
                <td className="px-4 py-3 text-slate-600">{row.ready_for_posting_count}</td>
                <td className="px-4 py-3 text-slate-600">
                  {row.most_common_exception_type ? labelStatus(row.most_common_exception_type) : "-"}
                </td>
              </tr>
            ))}
            {!loading && rows.length === 0 ? (
              <tr>
                <td className="px-4 py-8 text-center text-slate-500" colSpan={11}>
                  No carrier analytics yet.
                </td>
              </tr>
            ) : null}
            {loading ? (
              <tr>
                <td className="px-4 py-8 text-center text-slate-500" colSpan={11}>
                  Loading carrier analytics...
                </td>
              </tr>
            ) : null}
          </tbody>
        </table>
      </div>
      {error ? <div className="mt-3 rounded-md bg-red-50 px-3 py-2 text-sm text-red-700">{error}</div> : null}
    </section>
  );
}

function Notifications() {
  const [records, setRecords] = useState<NotificationRecord[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const loadNotifications = useCallback(async () => {
    try {
      setError(null);
      const data = await requestJson<NotificationRecord[]>("/notifications");
      setRecords(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load notifications");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadNotifications();
  }, [loadNotifications]);

  return (
    <section>
      <div className="mb-6 flex flex-wrap items-center justify-between gap-3">
        <div>
          <h1 className="text-2xl font-semibold text-slate-950">Notifications</h1>
          <p className="mt-1 text-sm text-slate-500">Last 10 batch and shipment SLA notifications.</p>
        </div>
        <button
          type="button"
          onClick={() => void loadNotifications()}
          className="rounded-md border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-50"
        >
          Refresh
        </button>
      </div>

      <div className="overflow-hidden rounded-lg border border-slate-200 bg-white">
        <table className="w-full border-collapse text-left text-sm">
          <thead className="bg-slate-50 text-xs uppercase text-slate-500">
            <tr>
              <th className="px-4 py-3 font-semibold">Occurred</th>
              <th className="px-4 py-3 font-semibold">Type</th>
              <th className="px-4 py-3 font-semibold">Details</th>
              <th className="px-4 py-3 font-semibold">Delivery</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-100">
            {records.map((record) => (
              <tr key={`${record.kind}-${record.id}`}>
                <td className="px-4 py-3 font-medium text-slate-900">
                  {formatDate(record.kind === "shipment_exception" ? record.occurred_at : record.sent_at)}
                </td>
                <td className="px-4 py-3 text-slate-600">
                  {record.kind === "shipment_exception" ? "Shipment SLA" : "Batch summary"}
                </td>
                <td className="px-4 py-3 text-slate-600">
                  {record.kind === "shipment_exception"
                    ? `${record.load_number}: ${labelStatus(record.transition)} — ${record.missing_docs.length > 0 ? record.missing_docs.map(labelStatus).join(", ") : "documents complete"}`
                    : `${record.total_count} processed, ${record.awaiting_review_count} need review, ${record.ready_for_posting_count} ready to post`}
                </td>
                <td className="px-4 py-3 text-slate-600">
                  {record.kind === "shipment_exception" ? labelStatus(record.notification_status) : "sent"}
                </td>
              </tr>
            ))}
            {!loading && records.length === 0 ? (
              <tr>
                <td className="px-4 py-8 text-center text-slate-500" colSpan={4}>
                  No notification records yet.
                </td>
              </tr>
            ) : null}
            {loading ? (
              <tr>
                <td className="px-4 py-8 text-center text-slate-500" colSpan={4}>
                  Loading notifications...
                </td>
              </tr>
            ) : null}
          </tbody>
        </table>
      </div>
      {error ? <div className="mt-3 rounded-md bg-red-50 px-3 py-2 text-sm text-red-700">{error}</div> : null}
    </section>
  );
}

function formatDuration(seconds: number | null): string {
  if (seconds === null) return "-";
  if (seconds < 60) return `${Math.round(seconds)}s`;
  if (seconds < 3600) return `${Math.round(seconds / 60)}m`;
  return `${(seconds / 3600).toFixed(1)}h`;
}

function QueueOperations() {
  const [entries, setEntries] = useState<DLQEntry[]>([]);
  const [nextCursor, setNextCursor] = useState<string | null>(null);
  const [metrics, setMetrics] = useState<QueueMetrics | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [activeReplay, setActiveReplay] = useState<string | null>(null);
  const [replayRequests, setReplayRequests] = useState<Record<string, string>>({});
  const [purgeBefore, setPurgeBefore] = useState("");
  const [purging, setPurging] = useState(false);
  const [operationMessage, setOperationMessage] = useState<string | null>(null);

  const loadMetrics = useCallback(async () => {
    setMetrics(await requestJson<QueueMetrics>("/operations/queue/metrics"));
  }, []);

  const loadFirstPage = useCallback(async () => {
    const page = await requestJson<DLQPage>("/operations/queue/dlq?limit=25");
    setEntries(page.items);
    setNextCursor(page.next_cursor);
  }, []);

  const refresh = useCallback(async () => {
    try {
      setError(null);
      await Promise.all([loadFirstPage(), loadMetrics()]);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load queue operations");
    } finally {
      setLoading(false);
    }
  }, [loadFirstPage, loadMetrics]);

  useEffect(() => {
    void refresh();
    const intervalId = window.setInterval(() => {
      void loadMetrics().catch(() => undefined);
    }, 10_000);
    return () => window.clearInterval(intervalId);
  }, [loadMetrics, refresh]);

  async function loadOlder() {
    if (!nextCursor) return;
    try {
      setError(null);
      const page = await requestJson<DLQPage>(
        `/operations/queue/dlq?limit=25&cursor=${encodeURIComponent(nextCursor)}`,
      );
      setEntries((current) => [...current, ...page.items]);
      setNextCursor(page.next_cursor);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load older DLQ entries");
    }
  }

  async function replay(entry: DLQEntry) {
    const requestId = replayRequests[entry.dlq_id] ?? crypto.randomUUID();
    setReplayRequests((current) => ({ ...current, [entry.dlq_id]: requestId }));
    setActiveReplay(entry.dlq_id);
    setOperationMessage(null);
    try {
      const result = await requestJson<{ live_stream_id: string; created: boolean }>(
        `/operations/queue/dlq/${encodeURIComponent(entry.dlq_id)}/replay`,
        { method: "POST", headers: { "Idempotency-Key": requestId } },
      );
      setReplayRequests((current) => {
        const next = { ...current };
        delete next[entry.dlq_id];
        return next;
      });
      setOperationMessage(
        `${result.created ? "Replay enqueued" : "Existing replay returned"}: ${result.live_stream_id}. Enqueueing does not mean reprocessing succeeded.`,
      );
      await refresh();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Replay request failed");
    } finally {
      setActiveReplay(null);
    }
  }

  async function purge() {
    if (!purgeBefore) return;
    const cutoff = new Date(purgeBefore);
    if (Number.isNaN(cutoff.getTime())) {
      setError("Enter a valid purge cutoff");
      return;
    }
    const cutoffIso = cutoff.toISOString();
    if (!window.confirm(`Permanently purge DLQ entries created before ${cutoffIso}?`)) return;
    setPurging(true);
    setOperationMessage(null);
    try {
      const result = await requestJson<PurgeResult>("/operations/queue/dlq/purge", {
        method: "POST",
        body: JSON.stringify({ before: cutoffIso }),
      });
      setOperationMessage(
        `Purged ${result.purged_count} entr${result.purged_count === 1 ? "y" : "ies"}.${
          result.has_more ? " More eligible entries remain; submit the same cutoff again." : ""
        }`,
      );
      await refresh();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Purge request failed");
    } finally {
      setPurging(false);
    }
  }

  const metricCards = [
    ["Live stream", metrics?.live_stream_length ?? "-"],
    ["Pending", metrics?.pending_count ?? "-"],
    ["Oldest pending", formatDuration(metrics?.oldest_pending_age_seconds ?? null)],
    ["Dead letters", metrics?.dlq_count ?? "-"],
  ];

  return (
    <section>
      <div className="mb-6 flex flex-wrap items-start justify-between gap-3">
        <div>
          <h1 className="text-2xl font-semibold text-slate-950">Queue Operations</h1>
          <p className="mt-1 text-sm text-slate-500">
            Trusted local-demo controls. Replay and retention are always explicit.
          </p>
        </div>
        <button
          type="button"
          onClick={() => void refresh()}
          className="rounded-md border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-50"
        >
          Refresh
        </button>
      </div>

      <div className="mb-6 grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
        {metricCards.map(([label, value]) => (
          <div key={label} className="rounded-lg border border-slate-200 bg-white p-4">
            <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">{label}</div>
            <div className="mt-2 text-2xl font-semibold text-slate-950">{value}</div>
          </div>
        ))}
      </div>

      <div className="mb-6 rounded-lg border border-slate-200 bg-slate-50 p-4">
        <div className="text-sm font-semibold text-slate-900">Age-based retention</div>
        <div className="mt-3 flex flex-wrap items-end gap-3">
          <label className="text-sm text-slate-700">
            Purge entries created before
            <input
              type="datetime-local"
              value={purgeBefore}
              onChange={(event) => setPurgeBefore(event.target.value)}
              className="mt-1 block rounded-md border border-slate-300 bg-white px-3 py-2 text-sm"
            />
          </label>
          <button
            type="button"
            disabled={!purgeBefore || purging}
            onClick={() => void purge()}
            className="rounded-md bg-red-600 px-4 py-2 text-sm font-medium text-white hover:bg-red-700 disabled:cursor-not-allowed disabled:bg-slate-400"
          >
            {purging ? "Purging..." : "Review and purge"}
          </button>
        </div>
      </div>

      {operationMessage ? (
        <div className="mb-4 rounded-md bg-blue-50 px-3 py-2 text-sm text-blue-800">{operationMessage}</div>
      ) : null}
      {error ? <div className="mb-4 rounded-md bg-red-50 px-3 py-2 text-sm text-red-700">{error}</div> : null}

      <div className="overflow-x-auto rounded-lg border border-slate-200 bg-white">
        <table className="w-full min-w-[1100px] border-collapse text-left text-sm">
          <thead className="bg-slate-50 text-xs uppercase text-slate-500">
            <tr>
              <th className="px-4 py-3 font-semibold">File / source</th>
              <th className="px-4 py-3 font-semibold">Failure</th>
              <th className="px-4 py-3 font-semibold">Attempts</th>
              <th className="px-4 py-3 font-semibold">Failed</th>
              <th className="px-4 py-3 font-semibold">Stream IDs</th>
              <th className="px-4 py-3 font-semibold">Replay</th>
              <th className="px-4 py-3 font-semibold">Action</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-100">
            {entries.map((entry) => {
              const retryingAmbiguousRequest = replayRequests[entry.dlq_id] !== undefined;
              const replayInProgress =
                entry.replay.state === "enqueued" ||
                entry.replay.state === "processing" ||
                entry.replay.state === "retrying";
              return (
                <tr key={entry.dlq_id} className="align-top">
                  <td className="px-4 py-3">
                    <div className="font-medium text-slate-900">{entry.filename ?? "Unknown file"}</div>
                    <div className="mt-1 text-xs text-slate-500">
                      {entry.source.gmail_account ?? "Unknown account"} · message {entry.source.message_id ?? "-"} · part {entry.source.mime_part_id ?? "-"}
                    </div>
                  </td>
                  <td className="max-w-sm px-4 py-3 text-slate-700">
                    {entry.failure_reason ?? "No reason recorded"}
                    {!entry.replayable ? (
                      <div className="mt-1 text-xs font-medium text-red-700">{entry.replay_block_reason}</div>
                    ) : null}
                  </td>
                  <td className="px-4 py-3 text-slate-600">{entry.attempt_count}</td>
                  <td className="px-4 py-3 text-slate-600">{formatDate(entry.failed_at)}</td>
                  <td className="px-4 py-3 font-mono text-xs text-slate-600">
                    <div>DLQ {entry.dlq_id}</div>
                    <div>Original {entry.original_stream_id ?? "-"}</div>
                    <div>New {entry.replay.last_live_stream_id ?? "-"}</div>
                  </td>
                  <td className="px-4 py-3 text-slate-600">
                    <div>{entry.replay.state ? <StatusBadge status={entry.replay.state} /> : "Never replayed"}</div>
                    <div className="mt-1 text-xs">Count: {entry.replay.count}</div>
                    {entry.replay.workflow_processing_status ? (
                      <div className="mt-1 text-xs">Workflow: {labelStatus(entry.replay.workflow_processing_status)}</div>
                    ) : null}
                  </td>
                  <td className="px-4 py-3">
                    <button
                      type="button"
                      disabled={
                        !entry.replayable ||
                        activeReplay === entry.dlq_id ||
                        (replayInProgress && !retryingAmbiguousRequest)
                      }
                      onClick={() => void replay(entry)}
                      className="rounded-md bg-slate-900 px-3 py-2 text-xs font-medium text-white hover:bg-slate-700 disabled:cursor-not-allowed disabled:bg-slate-300"
                    >
                      {activeReplay === entry.dlq_id
                        ? "Requesting..."
                        : retryingAmbiguousRequest
                          ? "Retry request"
                          : "Replay"}
                    </button>
                  </td>
                </tr>
              );
            })}
            {!loading && entries.length === 0 ? (
              <tr><td colSpan={7} className="px-4 py-8 text-center text-slate-500">No dead-letter entries.</td></tr>
            ) : null}
            {loading ? (
              <tr><td colSpan={7} className="px-4 py-8 text-center text-slate-500">Loading queue state...</td></tr>
            ) : null}
          </tbody>
        </table>
      </div>
      {nextCursor ? (
        <button
          type="button"
          onClick={() => void loadOlder()}
          className="mt-4 rounded-md border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-50"
        >
          Load older
        </button>
      ) : null}
    </section>
  );
}

export default function App() {
  const [activeView, setActiveView] = useState<View>("dashboard");
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);
  const [selectedShipmentId, setSelectedShipmentId] = useState<string | null>(null);

  function selectView(view: View) {
    if (view === "detail" && selectedRunId === null) {
      return;
    }
    if (view === "shipment_detail" && selectedShipmentId === null) {
      return;
    }
    setActiveView(view);
  }

  function openRun(runId: string) {
    setSelectedRunId(runId);
    setActiveView("detail");
  }

  function openShipment(shipmentId: string) {
    setSelectedShipmentId(shipmentId);
    setActiveView("shipment_detail");
  }

  async function openShipmentByLoadNumber(loadNumber: string) {
    const shipments = await requestJson<ShipmentSummary[]>("/shipments");
    const shipment = shipments.find((item) => item.load_number === loadNumber);
    if (shipment) {
      openShipment(shipment.id);
    }
  }

  return (
    <div className="flex min-h-screen bg-slate-100">
      <Sidebar
        activeView={activeView}
        hasSelectedRun={selectedRunId !== null}
        hasSelectedShipment={selectedShipmentId !== null}
        onSelect={selectView}
      />
      <main className="min-w-0 flex-1 bg-white px-8 py-7">
        {activeView === "dashboard" ? <Dashboard onOpenShipment={openShipment} /> : null}
        {activeView === "runs" ? <Runs onOpenRun={openRun} /> : null}
        {activeView === "detail" && selectedRunId ? (
          <RunDetail
            runId={selectedRunId}
            onBack={() => setActiveView("runs")}
            onViewShipment={(loadNumber) => void openShipmentByLoadNumber(loadNumber)}
          />
        ) : null}
        {activeView === "shipment_detail" && selectedShipmentId ? (
          <ShipmentDetail shipmentId={selectedShipmentId} onBack={() => setActiveView("dashboard")} />
        ) : null}
        {activeView === "analytics" ? <CarrierAnalytics /> : null}
        {activeView === "notifications" ? <Notifications /> : null}
        {activeView === "queue" ? <QueueOperations /> : null}
      </main>
    </div>
  );
}
