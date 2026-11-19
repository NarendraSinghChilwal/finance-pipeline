-- Technical reconciliation: Bronze must have exactly as many rows as RAW for every table.
-- Bronze is 1:1 with RAW (no filtering, no dedupe), so any difference means rows were lost or duplicated.
-- A singular test passes when it returns ZERO rows; each returned row is a table whose counts don't match.

with counts as (
    select 'invoices' as table_name,
           (select count(*) from {{ source('raw', 'invoices') }})       as raw_rows,
           (select count(*) from {{ ref('bronze_invoices') }})          as bronze_rows
    union all
    select 'purchase_orders',
           (select count(*) from {{ source('raw', 'purchase_orders') }}),
           (select count(*) from {{ ref('bronze_purchase_orders') }})
    union all
    select 'receipts',
           (select count(*) from {{ source('raw', 'receipts') }}),
           (select count(*) from {{ ref('bronze_receipts') }})
    union all
    select 'suppliers',
           (select count(*) from {{ source('raw', 'suppliers') }}),
           (select count(*) from {{ ref('bronze_suppliers') }})
)

select *
from counts
where raw_rows <> bronze_rows
