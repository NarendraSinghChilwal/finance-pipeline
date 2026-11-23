select
    invoice_id,
    supplier_id,
    po_id,
    invoice_amount,
    po_currency,
    invoice_currency,
    invoice_date,
    due_date,
    status,
    COUNT(*) OVER (PARTITION BY invoice_id) AS submission_count,
    ingested_at,
    _source_file,
    _source_row_number,
    _loaded_at
from {{ ref('bronze_invoices') }}

QUALIFY ROW_NUMBER() OVER (PARTITION BY invoice_id ORDER BY ingested_at, _source_row_number) = 1

