-- Silver invoices: one row per invoice_id (re-submissions removed, first kept).

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
    
    COUNT(*) OVER (PARTITION BY invoice_id) AS submission_count, 
    
    ingested_at,
    _source_file,
    _source_row_number,
    _loaded_at
from {{ ref('bronze_invoices') }}


-- Keep the first submission per invoice. Duplicates are exact copies with the same
-- ingested_at, so _source_row_number is the deterministic tiebreaker.

QUALIFY ROW_NUMBER() OVER (PARTITION BY invoice_id ORDER BY ingested_at, _source_row_number) = 1

