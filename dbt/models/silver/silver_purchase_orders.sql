select
    po_id,
    supplier_id,
    quantity,
    unit_price,
    line_total,
    currency,
    po_date,
    expected_delivery_date,
    status,
    flag as row_number() OVER (PARTITION BY po_id ORDER BY ingested_at DESC) = 1,
    ingested_at,
    _source_file,
    _source_row_number,
    _loaded_at
from {{ source('raw', 'purchase_orders') }}