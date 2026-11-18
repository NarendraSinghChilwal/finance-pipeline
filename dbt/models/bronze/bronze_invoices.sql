select
    invoice_id,
    supplier_id,
    po_id,
    try_cast(invoice_amount as number(18,2)) as invoice_amount,
    po_currency,
    invoice_currency,
    try_cast(invoice_date as date)           as invoice_date,
    try_cast(due_date as date)               as due_date,
    status,
    try_cast(ingested_at as timestamp_tz)    as ingested_at,
    _source_file,
    _source_row_number,
    _loaded_at
from {{ source('raw', 'invoices') }}