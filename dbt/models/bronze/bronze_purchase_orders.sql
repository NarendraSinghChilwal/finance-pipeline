select
    po_id,
    supplier_id,
    try_cast(quantity as number(38,0))              as quantity,
    try_cast(unit_price as number(18,2))            as unit_price,
    try_cast(line_total as number(18,2))            as line_total,
    currency,
    try_cast(po_date as date)                       as po_date,
    try_cast(expected_delivery_date as date)        as expected_delivery_date,
    status,
    try_cast(ingested_at as timestamp_tz)           as ingested_at,
    _source_file,
    _source_row_number,
    _loaded_at
from {{ source('raw', 'purchase_orders') }}