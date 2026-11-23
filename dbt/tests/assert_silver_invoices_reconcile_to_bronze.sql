-- Technical reconciliation: Silver dedupe must not lose any invoice submissions.
-- Silver keeps one row per invoice and stores how many copies existed in submission_count,
-- so SUM(submission_count) must equal the Bronze row count. Zero rows returned = pass.

with bronze as (
    select count(*) as bronze_rows
    from {{ ref('bronze_invoices') }}
),

silver as (
    select sum(submission_count) as silver_submissions
    from {{ ref('silver_invoices') }}
)

select *
from bronze, silver
where bronze_rows != silver_submissions