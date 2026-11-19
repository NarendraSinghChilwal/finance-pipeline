-- Business rule: an invoice cannot be due before it was issued.
-- Returns the bad rows. Zero rows = pass.

select 
    invoice_id,
    invoice_date,
    due_date
from {{ ref('bronze_invoices') }}
where due_date < invoice_date