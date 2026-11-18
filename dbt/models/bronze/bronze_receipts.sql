select
    receipt_id,
    po_id,
    supplier_id,
    try_cast(quantity_received as number(38,0))     as quantity_received,
    try_cast(receipt_date as date)                  as receipt_date,
    received_by,
    try_cast(ingested_at as timestamp_tz)           as ingested_at,
    _source_file,
    _source_row_number,
    _loaded_at
from {{ source('raw', 'receipts') }}