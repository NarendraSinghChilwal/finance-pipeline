-- Technical reconciliation: SCD2 keeps every supplier version, nothing is dropped.
-- So Silver must have exactly as many rows as Bronze (unlike invoices, where rows are
-- folded and SUM(submission_count) is compared instead). Zero rows returned = pass.


with bronze as(
    select
        count(*) as bronze_count
    from {{ ref('bronze_suppliers') }}
),

silver as(
    select 
        count(*) as silver_count
    from {{ ref('silver_suppliers') }}
)

select *
from bronze, silver
where bronze_count != silver_count