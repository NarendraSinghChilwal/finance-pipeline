"""
generate_raw_data.py

Synthetic data generator for the Finance Reconciliation Pipeline.

Produces five datasets exactly as they would arrive from real operational
source systems in a finance department — no pre-computed flags, no
anomaly labels, no hints. Raw data is always messy by nature, not by
annotation. The transformation layers (Silver, Gold) earn their existence
by detecting the problems themselves.

    Batch (CSV → S3 → Snowflake via COPY INTO):
        suppliers       — vendor master / dimension
        purchase_orders — what procurement ordered
        receipts        — what the warehouse confirmed receiving
        invoices        — what the supplier billed

    Streaming (JSONL → local now, Kafka in Phase 5):
        payments        — what finance actually paid out

Anomalies present in the data (unlabelled — Gold detects them):
    - Quantity received != quantity ordered  (short/over shipments)
    - Invoice amount != PO line total        (rounding, FX, billing errors)
    - Invoice currency != PO currency        (international supplier billing)
    - Invoice with no PO reference           (maverick spend, missing paperwork)
    - Invoice referencing a cancelled PO     (supplier missed cancellation notice)
    - Same invoice submitted twice           (supplier AP system re-send)
    - Payment after due date                 (late payment)
    - Partial payment across two tranches    (split payment runs)
    - Supplier terms/bank details changed    (SCD2 source — same supplier_id,
                                              new payment_terms or bank_account,
                                              later updated_at)

Default volumes (interview-credible, generates in seconds):
    200 suppliers  →  ~230 rows (with SCD2 drift)
    5,000 POs
    ~3,300 receipts
    6,000 invoices →  ~6,180 rows (with re-submissions)
    ~2,000 payment events → ~2,200 rows (with partial tranches)

Usage:
    python scripts/generate_raw_data.py                  # full run + S3 upload
    python scripts/generate_raw_data.py --local-only     # local inspection only
    python scripts/generate_raw_data.py --num-pos 10000  # custom volumes
    python scripts/generate_raw_data.py --seed 42        # reproducible run

Requires (requirements.txt):
    faker
    boto3
    python-dotenv

Requires in .env:
    LOADER_AWS_ACCESS_KEY_ID=...
    LOADER_AWS_SECRET_ACCESS_KEY=...
    LOADER_AWS_REGION=eu-west-1
    S3_LANDING_BUCKET=finance-pipeline-ingest-2026
"""

import argparse
import csv
import json
import logging
import os
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from faker import Faker

# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------
load_dotenv()
fake = Faker()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Anomaly injection rates
# These control HOW OFTEN real-world messiness occurs in the raw data.
# They are not flags — they only influence what raw field values get emitted.
# ---------------------------------------------------------------------------
QUANTITY_VARIANCE_RATE    = 0.08   # receipt qty drifts from PO qty
INVOICE_AMOUNT_DRIFT_RATE = 0.05   # invoice amount differs from PO line total
CURRENCY_DRIFT_RATE       = 0.06   # invoice billed in different currency than PO
MISSING_PO_REF_RATE       = 0.04   # invoice arrives with no po_id
CANCELLED_PO_INVOICE_RATE = 0.03   # invoice references a cancelled PO
DUPLICATE_INVOICE_RATE    = 0.03   # supplier re-submits same invoice
LATE_PAYMENT_RATE         = 0.06   # payment clears after due date
PARTIAL_PAYMENT_RATE      = 0.08   # payment split across two tranches
SUPPLIER_DRIFT_RATE       = 0.15   # supplier changes terms or bank details (SCD2)

# ---------------------------------------------------------------------------
# Reference data — mirrors realistic finance master data
# ---------------------------------------------------------------------------
CURRENCIES    = ["EUR", "USD", "GBP"]
PAYMENT_TERMS = ["NET_30", "NET_60", "NET_90", "IMMEDIATE"]
PO_STATUSES   = ["OPEN", "CLOSED", "CANCELLED"]

# Real invoice statuses from an AP (accounts payable) system
# PENDING   = received, not yet reviewed
# APPROVED  = matched to PO, cleared for payment
# DISPUTED  = finance raised a query (amount mismatch, missing PO, etc.)
# PAID      = payment issued
INVOICE_STATUSES = ["PENDING", "APPROVED", "DISPUTED", "PAID"]

PAYMENT_METHODS = ["BANK_TRANSFER", "DIRECT_DEBIT", "CHEQUE"]

RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
RUN_DT = datetime.now(timezone.utc).strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def uid() -> str:
    # random.getrandbits is seedable; uuid.uuid4() is not (it reads os.urandom).
    # Version-4 bits are still set, so these are valid UUIDv4 strings.
    return str(uuid.UUID(int=random.getrandbits(128), version=4))


def set_seed(seed: int | None) -> None:
    """Make a run reproducible. Seeds both `random` and Faker's shared generator."""
    if seed is None:
        return
    random.seed(seed)
    Faker.seed(seed)


def unique_ids(rows: list[dict], key: str) -> list[str]:
    """Order-preserving dedupe. A set would make iteration order depend on
    PYTHONHASHSEED and silently break --seed reproducibility."""
    return list(dict.fromkeys(r[key] for r in rows))


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def rand_date(start_days_ago: int, end_days_ago: int = 0) -> datetime:
    delta = random.randint(end_days_ago, start_days_ago)
    return datetime.now(timezone.utc) - timedelta(days=delta)


def other_currency(currency: str) -> str:
    """Pick a different currency — simulates cross-border billing mismatch."""
    return random.choice([c for c in CURRENCIES if c != currency])


def write_csv(records: list[dict], dataset: str, out_dir: Path) -> Path:
    if not records:
        raise ValueError(f"No records for {dataset}")
    folder = out_dir / dataset / RUN_DT
    folder.mkdir(parents=True, exist_ok=True)
    filepath = folder / f"{dataset}_{RUN_ID}.csv"
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=records[0].keys())
        writer.writeheader()
        writer.writerows(records)
    log.info("Wrote %d rows → %s", len(records), filepath)
    return filepath


def write_jsonl(records: list[dict], dataset: str, out_dir: Path) -> Path:
    folder = out_dir / dataset / RUN_DT
    folder.mkdir(parents=True, exist_ok=True)
    filepath = folder / f"{dataset}_{RUN_ID}.jsonl"
    with open(filepath, "w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")
    log.info("Wrote %d events → %s", len(records), filepath)
    return filepath


def upload_to_s3(local_path: Path, bucket: str, dataset: str) -> str:
    s3 = boto3.client(
        "s3",
        aws_access_key_id     = os.environ["LOADER_AWS_ACCESS_KEY_ID"],
        aws_secret_access_key = os.environ["LOADER_AWS_SECRET_ACCESS_KEY"],
        region_name           = os.environ.get("LOADER_AWS_REGION", "eu-west-1"),
    )
    key = f"{dataset}/{RUN_DT}/{local_path.name}"
    try:
        s3.upload_file(str(local_path), bucket, key)
        uri = f"s3://{bucket}/{key}"
        log.info("Uploaded → %s", uri)
        return uri
    except ClientError as exc:
        log.error("S3 upload failed for %s: %s", local_path.name, exc)
        raise


# ---------------------------------------------------------------------------
# Dataset builders
# ---------------------------------------------------------------------------

def build_suppliers(n: int) -> list[dict]:
    """
    SUPPLIER MASTER (Vendor Master in SAP terminology)

    In real finance systems this is the canonical record of every company
    your organisation buys from. Key fields:

        supplier_id    — internal unique key, generated by your ERP (SAP/Oracle)
        supplier_name  — legal trading name, used on invoices and remittances
        contact_email  — AP contact at the supplier for query resolution
        country        — drives VAT treatment, withholding tax rules
        currency       — supplier's preferred billing currency (not always honoured)
        payment_terms  — contractually agreed terms: NET_30 means pay within 30 days
                         of invoice date. NET_60/90 for larger suppliers with leverage.
                         IMMEDIATE for utilities or one-off purchases.
        bank_account   — IBAN for SEPA bank transfer (EU standard)
        is_active      — soft delete flag; deactivated rather than deleted for
                         audit trail reasons (you need history of who you paid)
        created_at     — when this supplier was first onboarded
        updated_at     — last time any attribute changed

    SCD Type 2 pattern:
        ~15% of suppliers reappear in this feed with a changed payment_terms
        or bank_account and a later updated_at. In a real system this means
        the supplier's AP team updated their record in the ERP. Your Silver
        dbt snapshot will detect the changed attribute and version the row —
        closing the old record and opening a new one — so you have a full
        history of what terms were in effect when each invoice was paid.
        This matters for audit: if a supplier changed their bank account
        and a payment went to the old account, you need to prove which
        record was live at payment time.
    """
    rows = []
    for _ in range(n):
        sid          = uid()
        currency     = random.choice(CURRENCIES)
        created_at   = rand_date(start_days_ago=730, end_days_ago=90)
        payment_term = random.choice(PAYMENT_TERMS)

        base = {
            "supplier_id":   sid,
            "supplier_name": fake.company(),
            "contact_email": fake.company_email(),
            "country":       fake.country(),
            "currency":      currency,
            "payment_terms": payment_term,
            "bank_account":  fake.iban(),
            "is_active":     True,
            "created_at":    created_at.isoformat(),
            "updated_at":    created_at.isoformat(),
        }
        rows.append(base)

        # SCD2 source: same supplier_id, changed terms or bank, later updated_at
        if random.random() < SUPPLIER_DRIFT_RATE:
            updated = dict(base)
            updated["payment_terms"] = random.choice(
                [t for t in PAYMENT_TERMS if t != payment_term]
            )
            updated["bank_account"] = fake.iban()
            updated["updated_at"]   = (
                created_at + timedelta(days=random.randint(30, 600))
            ).isoformat()
            rows.append(updated)

    log.info(
        "Built %d supplier rows (%d unique, %d SCD2 updates)",
        len(rows), n, len(rows) - n,
    )
    return rows


def build_purchase_orders(n: int, supplier_ids: list[str]) -> list[dict]:
    """
    PURCHASE ORDERS

    A PO is raised by a procurement team before anything is bought.
    It is a formal commitment: 'we agree to buy X units of Y at price Z
    from supplier S.' Nothing should be received or invoiced without one —
    but in practice, invoices arrive without PO references all the time
    (maverick spend, urgent purchases bypassing procurement).

    Key fields:

        po_id          — unique reference, printed on all related documents.
                         Suppliers are supposed to quote this on their invoices.
        supplier_id    — which supplier this order was placed with
        quantity       — units ordered
        unit_price     — agreed price per unit at time of order
        line_total     — quantity × unit_price. This is the baseline for
                         3-way match: receipt qty and invoice amount should
                         both reconcile back to this number.
        currency       — the currency the PO was raised in (may differ from
                         what the supplier invoices in — that's a real problem)
        po_date        — when procurement raised the order
        expected_delivery_date — agreed delivery window
        status         — OPEN (outstanding), CLOSED (fulfilled), CANCELLED

    In real ERP systems (SAP MM, Oracle Purchasing) a PO goes through an
    approval workflow before it's issued. Once CLOSED it can still be
    invoiced against for a short window. Once CANCELLED it should receive
    no further invoices — but suppliers don't always notice in time.
    """
    rows = []
    for _ in range(n):
        quantity   = random.randint(10, 500)
        unit_price = round(random.uniform(5.0, 250.0), 2)
        po_date    = rand_date(start_days_ago=180, end_days_ago=10)

        rows.append({
            "po_id":                  uid(),
            "supplier_id":            random.choice(supplier_ids),
            "quantity":               quantity,
            "unit_price":             unit_price,
            "line_total":             round(quantity * unit_price, 2),
            "currency":               random.choice(CURRENCIES),
            "po_date":                po_date.date().isoformat(),
            "expected_delivery_date": (
                po_date + timedelta(days=random.randint(7, 45))
            ).date().isoformat(),
            "status":                 random.choice(PO_STATUSES),
            "ingested_at":            now_iso(),
        })

    log.info("Built %d purchase orders", len(rows))
    return rows


def build_receipts(purchase_orders: list[dict]) -> list[dict]:
    """
    GOODS RECEIPT NOTES (GRNs)

    A receipt is raised by the warehouse or receiving team when goods
    actually arrive. It confirms physical delivery and is the second
    leg of the 3-way match:

        PO (what we ordered) ↔ GRN (what we received) ↔ Invoice (what we're billed)

    Key fields:

        receipt_id         — unique GRN reference
        po_id              — the PO this delivery fulfils
        supplier_id        — denormalised for query convenience
        quantity_received  — what the warehouse counted on arrival.
                             This should equal po.quantity — but short
                             shipments (supplier only sent 95 of 100) and
                             over-deliveries (sent 105) happen regularly.
                             ~8% of receipts here have a variance.
        receipt_date       — date goods were signed for
        received_by        — name of person who signed the delivery

    In real systems a GRN triggers the three-way match process in the ERP.
    If GRN qty != PO qty, the system flags it for procurement review before
    the invoice can be approved for payment. Finance cannot pay an invoice
    until the GRN exists and matches — this is the control.

    Cancelled POs get no receipt (nothing was delivered).
    """
    rows = []
    for po in purchase_orders:
        if po["status"] == "CANCELLED":
            continue

        po_qty = po["quantity"]

        # Short or over delivery — raw quantity variance, no flag
        if random.random() < QUANTITY_VARIANCE_RATE:
            variance  = random.uniform(-0.15, 0.15)
            recv_qty  = max(1, round(po_qty * (1 + variance)))
        else:
            recv_qty  = po_qty

        receipt_date = (
            datetime.fromisoformat(po["expected_delivery_date"])
            + timedelta(days=random.randint(-3, 10))
        )

        rows.append({
            "receipt_id":        uid(),
            "po_id":             po["po_id"],
            "supplier_id":       po["supplier_id"],
            "quantity_received": recv_qty,
            "receipt_date":      receipt_date.date().isoformat(),
            "received_by":       fake.name(),
            "ingested_at":       now_iso(),
        })

    log.info("Built %d goods receipt notes", len(rows))
    return rows


def build_invoices(
    purchase_orders: list[dict],
    supplier_ids: list[str],
    num_invoices: int,
) -> list[dict]:
    """
    SUPPLIER INVOICES (AP Invoices)

    An invoice is a payment demand from the supplier. In a well-run AP
    process every invoice should:
        - Quote a valid PO number
        - Be in the same currency as the PO
        - Have an amount matching the PO line total
        - Arrive once (not twice)
        - Reference a PO that hasn't been cancelled

    In reality, none of these are guaranteed. The anomalies present here
    (unlabelled — Gold detects them by comparing fields):

    Missing PO reference (~4%):
        po_id is NULL. Real causes: urgent purchase bypassed procurement,
        supplier forgot to quote the PO, subsidiary made a direct purchase.
        Finance has to manually find the matching PO or reject the invoice.

    Invoice against cancelled PO (~3%):
        Supplier didn't get the cancellation notice in time, or disputes
        the cancellation. Invoice arrives referencing a PO your system
        shows as CANCELLED. Needs manual review every time.

    Invoice amount differs from PO line total (~5%):
        Could be: FX rate difference (PO in EUR, invoice amount computed
        at a different rate), rounding on unit price × quantity, or a
        genuine billing error. Gold detects this by comparing
        invoice_amount to po.line_total.

    Currency mismatch (~6%):
        PO was raised in EUR but supplier invoices in USD. Common with
        US-headquartered suppliers who default to USD regardless of what
        the PO says. Gold detects this by comparing invoice_currency to
        po.currency.

    Duplicate submission (~3%):
        Same invoice_id appears twice in the file. Supplier's AP system
        re-sent a batch, or their ERP had a retry. Silver deduplication
        removes these; Gold should never see them.

    Key fields:
        invoice_id       — supplier's own reference number (not your internal ID)
        supplier_id      — who sent it
        po_id            — which PO it relates to (may be NULL)
        invoice_amount   — what they're asking to be paid
        po_currency      — currency of the originating PO (NULL if no PO)
        invoice_currency — currency the supplier billed in
        invoice_date     — date printed on the invoice
        due_date         — payment deadline (invoice_date + payment terms)
        status           — current state in the AP workflow
    """
    invoiceable_pos = [po for po in purchase_orders if po["status"] != "CANCELLED"]
    cancelled_pos   = [po for po in purchase_orders if po["status"] == "CANCELLED"]

    rows = []
    for _ in range(num_invoices):

        # Invoice against a cancelled PO
        if cancelled_pos and random.random() < CANCELLED_PO_INVOICE_RATE:
            po              = random.choice(cancelled_pos)
            po_id           = po["po_id"]
            supplier_id     = po["supplier_id"]
            base_amount     = po["line_total"]
            po_currency     = po["currency"]
            po_date         = datetime.fromisoformat(po["po_date"])

        # Orphaned invoice — no PO reference
        elif random.random() < MISSING_PO_REF_RATE:
            po_id           = None
            supplier_id     = random.choice(supplier_ids)
            base_amount     = round(random.uniform(100.0, 10000.0), 2)
            po_currency     = None
            po_date         = rand_date(start_days_ago=90, end_days_ago=5)

        # Normal invoice referencing a valid open/closed PO
        else:
            po              = random.choice(invoiceable_pos)
            po_id           = po["po_id"]
            supplier_id     = po["supplier_id"]
            base_amount     = po["line_total"]
            po_currency     = po["currency"]
            po_date         = datetime.fromisoformat(po["po_date"])

        # Currency mismatch — supplier bills in a different currency than the PO
        if po_currency and random.random() < CURRENCY_DRIFT_RATE:
            invoice_currency = other_currency(po_currency)
        else:
            invoice_currency = po_currency or random.choice(CURRENCIES)

        # Amount drift — invoice amount differs from PO line total
        if random.random() < INVOICE_AMOUNT_DRIFT_RATE:
            variance       = round(random.uniform(-50.0, 50.0), 2)
            invoice_amount = round(base_amount + variance, 2)
        else:
            invoice_amount = base_amount

        invoice_date = po_date + timedelta(days=random.randint(1, 30))
        due_date     = invoice_date + timedelta(days=30)

        rows.append({
            "invoice_id":       uid(),
            "supplier_id":      supplier_id,
            "po_id":            po_id,
            "invoice_amount":   invoice_amount,
            "po_currency":      po_currency,
            "invoice_currency": invoice_currency,
            "invoice_date":     invoice_date.date().isoformat(),
            "due_date":         due_date.date().isoformat(),
            "status":           random.choice(INVOICE_STATUSES),
            "ingested_at":      now_iso(),
        })

    # Duplicate submission — same row, same invoice_id, appears twice
    originals  = list(rows)
    num_dupes  = round(len(originals) * DUPLICATE_INVOICE_RATE)
    duplicates = random.sample(originals, num_dupes)
    rows.extend(duplicates)

    log.info(
        "Built %d invoice rows (%d unique + %d re-submissions)",
        len(rows), num_invoices, num_dupes,
    )
    return rows


def build_payments(invoices: list[dict]) -> list[dict]:
    """
    PAYMENT EVENTS (outgoing bank payments)

    A payment is the final step in the AP cycle — finance releases funds
    to the supplier. In real systems this is triggered by the ERP once
    the 3-way match is confirmed (PO + GRN + Invoice all agree).

    Only APPROVED or PAID invoices generate a payment in this run.
    PENDING and DISPUTED invoices are still in the AP workflow.

    Key fields:
        payment_id      — unique reference for this payment event.
                          This is what the Silver MERGE upserts on —
                          same payment_id arriving twice (Kafka at-least-once
                          delivery) is idempotent because MERGE on payment_id
                          updates rather than inserts a duplicate.
        invoice_id      — which invoice this payment settles
        supplier_id     — denormalised for query convenience
        amount_paid     — what was actually transferred to the supplier's bank
        invoice_amount  — the original demand (kept for Gold comparison)
        currency        — payment currency (should match invoice_currency)
        payment_date    — date funds left your bank account
        payment_method  — BANK_TRANSFER (most common for B2B), DIRECT_DEBIT,
                          CHEQUE (rare, legacy suppliers only)
        tranche         — 1 for a single full payment, 1 or 2 for split payments.
                          Gold detects partial settlement by summing amount_paid
                          per invoice_id and comparing to invoice_amount.
        event_timestamp — when this event was emitted (Kafka message timestamp)
        updated_at      — used by Silver MERGE to resolve which version is newer

    Anomalies present (unlabelled):
        Late payment (~6%):
            payment_date > invoice due_date. Finance teams track this because
            late payment of supplier invoices can trigger contractual penalties
            (the payment terms you agreed to) and damage supplier relationships.
            Gold detects this by comparing payment_date to invoice.due_date.

        Partial payment (~8%):
            Two payment events with different payment_ids but the same invoice_id,
            each covering a fraction of the invoice_amount. Real cause: finance
            runs a payment batch on Tuesday covering 60% of outstanding invoices
            by value, then a second batch on Friday covers the rest. Gold detects
            partial settlement by SUM(amount_paid) per invoice_id != invoice_amount.
    """
    payable          = [inv for inv in invoices if inv["status"] in ("APPROVED", "PAID")]
    seen_invoice_ids : set[str] = set()
    rows             : list[dict] = []

    for inv in payable:
        inv_id = inv["invoice_id"]
        if inv_id in seen_invoice_ids:
            continue
        seen_invoice_ids.add(inv_id)

        due_date       = datetime.fromisoformat(inv["due_date"])
        invoice_amount = inv["invoice_amount"]
        currency       = inv["invoice_currency"]

        # Late vs on-time payment
        if random.random() < LATE_PAYMENT_RATE:
            payment_date = due_date + timedelta(days=random.randint(30, 90))
        else:
            payment_date = due_date - timedelta(days=random.randint(0, 10))

        # Partial payment — two tranches, two payment events
        if random.random() < PARTIAL_PAYMENT_RATE:
            split          = round(random.uniform(0.4, 0.7), 2)
            amount_t1      = round(invoice_amount * split, 2)
            amount_t2      = round(invoice_amount - amount_t1, 2)
            tranche2_date  = payment_date + timedelta(days=random.randint(3, 14))
            event_ts       = now_iso()

            rows.append({
                "payment_id":      uid(),
                "invoice_id":      inv_id,
                "supplier_id":     inv["supplier_id"],
                "amount_paid":     amount_t1,
                "invoice_amount":  invoice_amount,
                "currency":        currency,
                "payment_date":    payment_date.date().isoformat(),
                "payment_method":  random.choice(PAYMENT_METHODS),
                "tranche":         1,
                "event_timestamp": event_ts,
                "updated_at":      event_ts,
            })
            rows.append({
                "payment_id":      uid(),
                "invoice_id":      inv_id,
                "supplier_id":     inv["supplier_id"],
                "amount_paid":     amount_t2,
                "invoice_amount":  invoice_amount,
                "currency":        currency,
                "payment_date":    tranche2_date.date().isoformat(),
                "payment_method":  random.choice(PAYMENT_METHODS),
                "tranche":         2,
                "event_timestamp": now_iso(),
                "updated_at":      now_iso(),
            })
        else:
            event_ts = now_iso()
            rows.append({
                "payment_id":      uid(),
                "invoice_id":      inv_id,
                "supplier_id":     inv["supplier_id"],
                "amount_paid":     invoice_amount,
                "invoice_amount":  invoice_amount,
                "currency":        currency,
                "payment_date":    payment_date.date().isoformat(),
                "payment_method":  random.choice(PAYMENT_METHODS),
                "tranche":         1,
                "event_timestamp": event_ts,
                "updated_at":      event_ts,
            })

    log.info("Built %d payment events", len(rows))
    return rows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Generate synthetic finance reconciliation data."
    )
    parser.add_argument("--num-suppliers", type=int, default=200)
    parser.add_argument("--num-pos",       type=int, default=5000)
    parser.add_argument("--num-invoices",  type=int, default=6000)
    parser.add_argument("--local-only",    action="store_true")
    parser.add_argument("--out-dir",       default="data/raw")
    parser.add_argument("--seed",          type=int, default=None,
                        help="Seed for reproducible output (used by the tests)")
    args = parser.parse_args()
    set_seed(args.seed)

    bucket  = os.environ.get("S3_LANDING_BUCKET")
    out_dir = Path(args.out_dir)

    if not args.local_only and not bucket:
        raise SystemExit(
            "S3_LANDING_BUCKET not set in .env. "
            "Run with --local-only or set the variable."
        )

    log.info("=== Finance Pipeline Generator | run_id=%s ===", RUN_ID)
    log.info(
        "Volumes → suppliers=%d  POs=%d  invoices=%d",
        args.num_suppliers, args.num_pos, args.num_invoices,
    )

    suppliers       = build_suppliers(args.num_suppliers)
    supplier_ids    = unique_ids(suppliers, "supplier_id")
    purchase_orders = build_purchase_orders(args.num_pos, supplier_ids)
    receipts        = build_receipts(purchase_orders)
    invoices        = build_invoices(purchase_orders, supplier_ids, args.num_invoices)
    payments        = build_payments(invoices)

    batch_datasets: dict[str, list[dict]] = {
        "suppliers":       suppliers,
        "purchase_orders": purchase_orders,
        "receipts":        receipts,
        "invoices":        invoices,
    }

    local_paths: dict[str, Path] = {}
    for name, records in batch_datasets.items():
        local_paths[name] = write_csv(records, name, out_dir)
    local_paths["payments"] = write_jsonl(payments, "payments", out_dir)

    if not args.local_only:
        log.info("Uploading to s3://%s ...", bucket)
        for name in batch_datasets:
            upload_to_s3(local_paths[name], bucket, name)
        log.info(
            "Payments → local only (%s). Kafka producer replaces this in Phase 5.",
            local_paths["payments"],
        )
    else:
        log.info("--local-only: skipping S3 upload.")

    log.info("=== Generation complete | run_id=%s ===", RUN_ID)


if __name__ == "__main__":
    main()