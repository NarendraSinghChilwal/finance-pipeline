# Phase 4 -- RAW landing tables (owned by SYSADMIN, written by LOADER_ROLE via COPY INTO).
#
# Design: RAW is "exactly as landed".
#   - Every business column is VARCHAR. A malformed date or amount can never fail a load, and the original value
#     is always preserved for audit. Typing (TRY_CAST) happens in dbt BRONZE, where bad values become testable data.
#   - Three audit columns on every table: which file, which row, when loaded. That's lineage back to S3.
#   - No primary keys or dedup here. RAW keeps duplicates on purpose (re-submitted invoices, SCD2 supplier rows,
#     and the same IDs from multiple generator runs); SILVER handles them.
#
# Column order MUST match the CSV header order -- COPY INTO maps by position ($1, $2, ...).
# snowflake_table is a preview resource: enabled via preview_features_enabled in snowflake.tf.

locals {
  raw_tables = {
    SUPPLIERS = [
      "supplier_id", "supplier_name", "contact_email", "country", "currency",
      "payment_terms", "bank_account", "is_active", "created_at", "updated_at",
    ]
    PURCHASE_ORDERS = [
      "po_id", "supplier_id", "quantity", "unit_price", "line_total",
      "currency", "po_date", "expected_delivery_date", "status", "ingested_at",
    ]
    RECEIPTS = [
      "receipt_id", "po_id", "supplier_id", "quantity_received",
      "receipt_date", "received_by", "ingested_at",
    ]
    INVOICES = [
      "invoice_id", "supplier_id", "po_id", "invoice_amount", "po_currency",
      "invoice_currency", "invoice_date", "due_date", "status", "ingested_at",
    ]
  }
}

resource "snowflake_table" "raw" {
  for_each = local.raw_tables

  database = snowflake_database.this.name
  schema   = snowflake_schema.this["RAW"].name
  name     = each.key
  comment  = "Landing table for s3://.../${lower(each.key)}/ -- all VARCHAR, loaded by COPY INTO from ${each.key}_STAGE"

  dynamic "column" {
    for_each = each.value
    content {
      name = upper(column.value)
      type = "VARCHAR"
    }
  }

  # --- audit / lineage columns (filled by the COPY INTO SELECT) ---
  column {
    name    = "_SOURCE_FILE"
    type    = "VARCHAR"
    comment = "METADATA$FILENAME -- S3 key the row came from"
  }
  column {
    name    = "_SOURCE_ROW_NUMBER"
    type    = "NUMBER(38,0)"
    comment = "METADATA$FILE_ROW_NUMBER -- line number within that file"
  }
  column {
    name    = "_LOADED_AT"
    type    = "TIMESTAMP_LTZ(9)"
    comment = "When COPY INTO loaded the row"
  }
}

output "raw_tables" {
  value = [for t in snowflake_table.raw : t.fully_qualified_name]
}
