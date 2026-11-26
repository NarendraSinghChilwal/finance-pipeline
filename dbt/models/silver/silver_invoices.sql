-- Silver invoices: one row per invoice_id (re-submissions removed, first kept).
-- Incremental: later runs reprocess only invoices touched by newly loaded rows,
-- but ALL rows for those invoices, so the first-kept copy and submission_count
-- are always computed on full history (a batch-only window would get both wrong).

{{ config(materialized='incremental', unique_key='invoice_id') }}

with source as (
    select * from {{ ref('bronze_invoices') }}

    {% if is_incremental() %}
    -- only invoices touched by new rows, but ALL their rows (old + new)
    where invoice_id in (
        select invoice_id
        from {{ ref('bronze_invoices') }}
        where _loaded_at > (select max(_loaded_at) from {{ this }})
    )
    {% endif %}
)

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

    -- How many times this invoice was submitted (1 = normal, >1 = re-submitted).
    -- Kept so Gold can flag duplicates, and so SUM(submission_count) reconciles to Bronze.
    count(*) over (partition by invoice_id) as submission_count,

    ingested_at,
    _source_file,
    _source_row_number,
    _loaded_at
from source

-- Keep the first submission per invoice. Duplicates are exact copies with the same
-- ingested_at, so _source_row_number is the deterministic tiebreaker.
qualify row_number() over (partition by invoice_id order by ingested_at, _source_row_number) = 1