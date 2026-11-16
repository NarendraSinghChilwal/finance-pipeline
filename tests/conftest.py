"""Shared fixtures.

The dataset is built once per test session from a fixed seed, so every
assertion runs against the same reproducible data.
"""
import pytest

import generate_raw_data as gen

SEED = 42


@pytest.fixture(scope="session")
def dataset() -> dict[str, list[dict]]:
    gen.set_seed(SEED)
    suppliers = gen.build_suppliers(100)
    supplier_ids = gen.unique_ids(suppliers, "supplier_id")
    purchase_orders = gen.build_purchase_orders(2000, supplier_ids)
    receipts = gen.build_receipts(purchase_orders)
    invoices = gen.build_invoices(purchase_orders, supplier_ids, 3000)
    payments = gen.build_payments(invoices)
    return {
        "suppliers": suppliers,
        "purchase_orders": purchase_orders,
        "receipts": receipts,
        "invoices": invoices,
        "payments": payments,
    }
