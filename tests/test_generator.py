"""Tests for the synthetic data generator.

They check the properties the rest of the pipeline relies on:
  * referential integrity between datasets, so Gold joins behave;
  * anomalies exist at roughly the intended rate, otherwise Gold has nothing to detect;
  * anomalies are NOT labelled -- raw data carries no hints (a design decision);
  * seeded runs are reproducible, including across processes.

Rate assertions use wide bands on purpose: they catch "this anomaly vanished"
or "this anomaly exploded", not statistical noise.
"""
import csv
import os
import subprocess
import sys
import uuid
from collections import Counter, defaultdict
from pathlib import Path

import pytest

import generate_raw_data as gen

SCRIPTS_DIR = str(Path(gen.__file__).resolve().parent)

# Columns the earlier generator stamped onto rows. Detecting these is Gold's job.
ANOMALY_LABEL_COLUMNS = {
    "has_missing_po",
    "has_amount_diff",
    "has_currency_mismatch",
    "has_cancelled_po_ref",
    "quantity_mismatch",
    "amount_discrepancy",
    "is_late",
    "is_partial",
}


# ---------------------------------------------------------------- reproducibility
def test_uid_is_a_valid_uuid4():
    assert uuid.UUID(gen.uid()).version == 4


def test_same_seed_gives_same_ids():
    gen.set_seed(123)
    first = [gen.uid() for _ in range(5)]
    gen.set_seed(123)
    second = [gen.uid() for _ in range(5)]
    assert first == second


def test_different_seeds_give_different_ids():
    gen.set_seed(1)
    a = gen.uid()
    gen.set_seed(2)
    b = gen.uid()
    assert a != b


def test_seeded_output_is_stable_across_processes():
    """Guards against hash-order leaks (e.g. iterating a set of strings)."""
    code = (
        "import hashlib, json, generate_raw_data as g;"
        "g.set_seed(7);"
        "s = g.build_suppliers(20);"
        "ids = g.unique_ids(s, 'supplier_id');"
        "po = g.build_purchase_orders(50, ids);"
        "rows = [(p['po_id'], p['supplier_id'], p['quantity'], p['unit_price']) for p in po];"
        "print(hashlib.md5(json.dumps(rows).encode()).hexdigest())"
    )
    outputs = []
    for hash_seed in ("1", "2"):
        env = {
            **os.environ,
            "PYTHONHASHSEED": hash_seed,
            "PYTHONPATH": os.pathsep.join(filter(None, [SCRIPTS_DIR, os.environ.get("PYTHONPATH")])),
        }
        result = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True, env=env
        )
        assert result.returncode == 0, result.stderr
        outputs.append(result.stdout.strip())
    assert outputs[0] == outputs[1]


# ---------------------------------------------------------------- design contract
def test_raw_data_carries_no_anomaly_labels(dataset):
    for name, rows in dataset.items():
        leaked = set(rows[0]) & ANOMALY_LABEL_COLUMNS
        assert not leaked, f"{name} has pre-computed anomaly columns: {leaked}"


# ---------------------------------------------------------------- suppliers
def test_suppliers_have_scd2_versions(dataset):
    by_id = defaultdict(list)
    for row in dataset["suppliers"]:
        by_id[row["supplier_id"]].append(row)

    assert len(by_id) == 100
    versioned = [rows for rows in by_id.values() if len(rows) > 1]
    assert 5 < len(versioned) < 30  # ~15% of 100

    for rows in versioned:
        assert len(rows) == 2
        old, new = rows
        assert new["updated_at"] > old["updated_at"]
        assert (new["payment_terms"], new["bank_account"]) != (
            old["payment_terms"],
            old["bank_account"],
        )


# ---------------------------------------------------------------- purchase orders
def test_po_ids_are_unique(dataset):
    ids = [po["po_id"] for po in dataset["purchase_orders"]]
    assert len(ids) == len(set(ids))


def test_po_line_total_is_quantity_times_price(dataset):
    for po in dataset["purchase_orders"]:
        assert po["line_total"] == pytest.approx(po["quantity"] * po["unit_price"], abs=0.01)


def test_pos_reference_known_suppliers(dataset):
    supplier_ids = {s["supplier_id"] for s in dataset["suppliers"]}
    assert all(po["supplier_id"] in supplier_ids for po in dataset["purchase_orders"])


# ---------------------------------------------------------------- receipts
def test_receipts_reference_real_pos(dataset):
    po_ids = {po["po_id"] for po in dataset["purchase_orders"]}
    assert all(r["po_id"] in po_ids for r in dataset["receipts"])


def test_no_receipts_for_cancelled_pos(dataset):
    cancelled = {po["po_id"] for po in dataset["purchase_orders"] if po["status"] == "CANCELLED"}
    assert cancelled, "fixture should contain cancelled POs"
    assert not any(r["po_id"] in cancelled for r in dataset["receipts"])


def test_some_receipts_differ_from_po_quantity(dataset):
    ordered = {po["po_id"]: po["quantity"] for po in dataset["purchase_orders"]}
    receipts = dataset["receipts"]
    off = sum(1 for r in receipts if r["quantity_received"] != ordered[r["po_id"]])
    assert 0.03 < off / len(receipts) < 0.13  # target ~8%


# ---------------------------------------------------------------- invoices
def test_resubmitted_invoices_are_exact_duplicates(dataset):
    invoices = dataset["invoices"]
    counts = Counter(i["invoice_id"] for i in invoices)
    duplicated = [inv_id for inv_id, n in counts.items() if n > 1]

    assert max(counts.values()) == 2
    assert len(duplicated) == round(len(counts) * gen.DUPLICATE_INVOICE_RATE)

    rows_by_id = defaultdict(list)
    for inv in invoices:
        rows_by_id[inv["invoice_id"]].append(inv)
    assert all(rows_by_id[inv_id][0] == rows_by_id[inv_id][1] for inv_id in duplicated)


def test_invoice_po_references_exist_when_present(dataset):
    po_ids = {po["po_id"] for po in dataset["purchase_orders"]}
    assert all(i["po_id"] in po_ids for i in dataset["invoices"] if i["po_id"] is not None)


def test_some_invoices_have_no_po_reference(dataset):
    invoices = dataset["invoices"]
    missing = [i for i in invoices if i["po_id"] is None]
    assert 0.02 < len(missing) / len(invoices) < 0.06  # target ~4%
    assert all(i["po_currency"] is None for i in missing)


def test_some_invoices_reference_cancelled_pos(dataset):
    cancelled = {po["po_id"] for po in dataset["purchase_orders"] if po["status"] == "CANCELLED"}
    hits = [i for i in dataset["invoices"] if i["po_id"] in cancelled]
    assert len(hits) > 0


def test_some_invoices_use_a_different_currency_than_the_po(dataset):
    with_po = [i for i in dataset["invoices"] if i["po_currency"] is not None]
    drifted = sum(1 for i in with_po if i["invoice_currency"] != i["po_currency"])
    assert 0.03 < drifted / len(with_po) < 0.10  # target ~6%


def test_some_invoice_amounts_differ_from_po_total(dataset):
    line_total = {po["po_id"]: po["line_total"] for po in dataset["purchase_orders"]}
    with_po = [i for i in dataset["invoices"] if i["po_id"] is not None]
    drifted = sum(1 for i in with_po if abs(i["invoice_amount"] - line_total[i["po_id"]]) > 0.005)
    assert 0.02 < drifted / len(with_po) < 0.09  # target ~5%


# ---------------------------------------------------------------- payments
def test_payments_only_for_approved_or_paid_invoices(dataset):
    status = {i["invoice_id"]: i["status"] for i in dataset["invoices"]}
    assert all(status[p["invoice_id"]] in {"APPROVED", "PAID"} for p in dataset["payments"])


def test_payment_ids_are_unique(dataset):
    ids = [p["payment_id"] for p in dataset["payments"]]
    assert len(ids) == len(set(ids))


def test_payments_add_up_to_the_invoice_amount(dataset):
    amount = {i["invoice_id"]: i["invoice_amount"] for i in dataset["invoices"]}
    paid = defaultdict(float)
    for p in dataset["payments"]:
        paid[p["invoice_id"]] += p["amount_paid"]
    for invoice_id, total in paid.items():
        assert total == pytest.approx(amount[invoice_id], abs=0.01)


def test_partial_payments_are_split_into_two_tranches(dataset):
    tranches = defaultdict(list)
    for p in dataset["payments"]:
        tranches[p["invoice_id"]].append(p["tranche"])

    assert all(sorted(t) in ([1], [1, 2]) for t in tranches.values())
    split = sum(1 for t in tranches.values() if len(t) == 2)
    assert 0.04 < split / len(tranches) < 0.13  # target ~8%


# ---------------------------------------------------------------- file output
def test_write_csv_uses_dataset_then_date_layout_and_roundtrips(tmp_path, dataset):
    path = gen.write_csv(dataset["suppliers"], "suppliers", tmp_path)

    assert path.parent.name == gen.RUN_DT
    assert path.parent.parent.name == "suppliers"
    with open(path, newline="", encoding="utf-8") as f:
        assert len(list(csv.DictReader(f))) == len(dataset["suppliers"])


def test_write_csv_rejects_empty_input(tmp_path):
    with pytest.raises(ValueError):
        gen.write_csv([], "suppliers", tmp_path)
