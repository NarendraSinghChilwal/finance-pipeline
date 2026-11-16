# Finance Reconciliation Pipeline -- Project Status & Handoff

Last updated: 2026-11-16. Read this first in any new chat, then continue from "Next steps".
No secrets live in this file or in git. Credentials are in `.env` (gitignored) and `~/.snowflake/` (outside the repo).

## What this project is
Synthetic finance reconciliation pipeline. Batch datasets (suppliers, purchase_orders, receipts, invoices) land in S3 and
load into Snowflake with COPY INTO. Payments arrive as a Kafka stream (MSK Serverless -> Python consumer -> Snowflake MERGE).
dbt builds Bronze -> Silver -> Gold on Snowflake (Gold = 3-way match PO <-> receipt <-> invoice, plus exception detection
against payments). Airflow orchestrates the data pipeline; GitHub Actions is a separate control plane for code CI/CD.
Terraform owns infrastructure. Goal: lead-level, production-patterned portfolio project for data engineering interviews.
Scope decision: transformation stays dbt on Snowflake. PySpark is deliberately out of scope (README "extensions" only).

## Phase status
| Phase | What | Status |
|---|---|---|
| 0 | Environment, git, gitignore, AWS budget alert | DONE |
| 1 | S3 bucket + IAM loader user (Terraform) | DONE |
| 3 | Synthetic data generator + data in S3 | DONE (pytest suite added, 24 tests) |
| 2 | Snowflake foundation (Terraform: warehouses, FINANCE_DEV, schemas, RBAC) | DONE (34 resources applied 2026-11-16, negative RBAC test passed) |
| 4 | Batch ingestion (S3 -> COPY INTO) | DONE except loader service user (load proven 2026-11-16, idempotent) |
| 5 | Streaming (MSK Serverless, producer/consumer, MERGE on payment_id) | not started |
| 6-9 | dbt sources/Bronze, Silver (incremental + SCD2 snapshot), Gold (3-way match), tests | not started |
| 10 | Airflow DAG (Docker) | not started |
| 11 | GitHub Actions CI/CD (dev/CI/prod databases) | not started |
| 12 | dbt docs, README with honest Status section | not started |

## Environment facts
- Machine: Mac (Apple silicon), zsh, VS Code terminal, Python 3.11 venv at `.venv`, project root `~/Desktop/finance pipeline`.
- Repo: github.com/NarendraSinghChilwal/finance-pipeline (branch main). Latest commit when written: a128a71.
- Terraform 1.15.x installed via `brew install hashicorp/tap/terraform`. State is LOCAL (gitignored). Saved plans (`tfplan`) are gitignored.
- AWS: region eu-west-1. Admin identity = IAM user `NarendraChilwal` (MFA), CLI profile `finance-pipeline`.
  Service identity = IAM user `finance-pipeline-loader` (S3 put/get/delete/list on ONE bucket only), keys in `.env` as LOADER_AWS_*.
- S3 bucket: `finance-pipeline-ingest-2026` (public access blocked, versioning on, SSE-S3). Layout: `<dataset>/<YYYY-MM-DD>/<dataset>_<runid>.csv`.
  Currently holds 8 objects: two runs (20260822T130416 and 20261116T141623), one file per batch dataset each.
  Phase 4 COPY INTO will load BOTH runs into RAW -- expected; the same business IDs appear once per run, so Silver must dedupe (or load one run only).
- AWS account is on a FREE PLAN with credits (ends ~21 Feb 2027). Check MSK Serverless is allowed before Phase 5.
- Snowflake: ENTERPRISE edition trial, AWS eu-west-1 (Ireland). Created 2026-11-16, 30-day trial (~$400 credits), ends ~2026-12-16.
  Account identifier: organization `VMYOYIJ`, account `EB08969`. Default session timezone is America/Los_Angeles (-0800) -- set explicitly later.
  Existing default warehouses (COMPUTE_WH, SNOWFLAKE_LEARNING_WH, SYSTEM$STREAMLIT_NOTEBOOK_*) all set to AUTO_SUSPEND=60. Leave them alone.
  Resource monitor `ACCOUNT_GUARD` (50 credits/month, notify 75%, suspend 90%, suspend now 100%) is set at ACCOUNT level.
  Created manually (needs ACCOUNTADMIN, which automation must not hold) -- document this in the README.
  Service user `TERRAFORM_SVC` (TYPE=SERVICE, key-pair only, no password) holds SYSADMIN + SECURITYADMIN.
  Private key `~/.snowflake/tf_key.p8` (never in repo/chat), public key `~/.snowflake/tf_key.pub`.
- Terraform files in `infra/`: providers.tf (ONE required_providers block: hashicorp/aws ~>5.0, snowflakedb/snowflake ~>2.0),
  snowflake.tf (two provider configs: default role SYSADMIN, alias `security_admin` role SECURITYADMIN), variables.tf, s3.tf, iam.tf,
  snowflake_objects.tf (SYSADMIN-owned objects), snowflake_rbac.tf (roles + grants via SECURITYADMIN). Terraform login as TERRAFORM_SVC PROVEN.
  snowflake_ingest.tf (Phase 4): IAM role finance-pipeline-snowflake-s3-read (read-only, trusts only the integration's IAM user + external ID),
  storage integration FINANCE_S3_INT (allowed location = the bucket only), 4 external stages RAW.<DATASET>_STAGE with inline CSV format
  (skip_header 1, quoted fields, null_if ['']), USAGE on stages to LOADER_ROLE only. ONE apply: the role ARN is built as a string, so the
  integration doesn't depend on the role resource (no cycle). snowflake_raw_tables.tf: RAW.SUPPLIERS / PURCHASE_ORDERS / RECEIPTS / INVOICES,
  all business columns VARCHAR + _SOURCE_FILE, _SOURCE_ROW_NUMBER, _LOADED_AT. snowflake_table is preview -> preview_features_enabled =
  ["snowflake_table_resource"] on the SYSADMIN provider only. Stage/integration resources used are the new stable ones (no preview flag).
- Bootstrap (ACCOUNTADMIN, by hand, recorded in sql/00_bootstrap_accountadmin.sql): GRANT CREATE INTEGRATION ON ACCOUNT TO ROLE SYSADMIN.
- RAW loaded 2026-11-16 by Narendra as LOADER_ROLE with sql/04_load_raw.sql: SUPPLIERS 467, PURCHASE_ORDERS 10000, RECEIPTS 6658,
  INVOICES 12360 -- each from 2 files (runs 20260822T130416 + 20261116T141623). Re-run = "0 files processed" (load metadata, 64 days).
  Same business IDs appear once per run -> Silver must dedupe on business key (latest _LOADED_AT / _SOURCE_FILE).
- SQL lives in sql/ (versioned); Snowsight worksheets are only where it is run.
- Phase 2 objects (all Terraform): warehouses LOADING_WH / TRANSFORMING_WH / REPORTING_WH (XSMALL, auto_suspend 60, auto_resume,
  initially suspended, statement timeout 1800s); database FINANCE_DEV (var `snowflake_database_name`, retention 1 day, PUBLIC dropped);
  schemas RAW, BRONZE, SILVER, GOLD (all MANAGED ACCESS -- dbt owns its tables but cannot grant on them; access is decided only in Terraform).
  Roles (all granted to SYSADMIN so no orphaned objects): LOADER_ROLE (LOADING_WH; USAGE RAW; INSERT+SELECT on all+future RAW tables),
  TRANSFORMER_ROLE (TRANSFORMING_WH; SELECT all+future RAW tables; USAGE+CREATE TABLE+CREATE VIEW on BRONZE/SILVER/GOLD),
  REPORTER_ROLE (REPORTING_WH; SELECT all+future tables and views in GOLD only). Object grants are schema-level, ALL + FUTURE pairs.
  Verified: CREATE TABLE in GOLD as REPORTER_ROLE fails with insufficient privileges (negative test).
- `.env` variable names: SNOWFLAKE_ACCOUNT/USER/PASSWORD (blank, unused -- Snowflake access is key-pair), AWS_PROFILE,
  LOADER_AWS_ACCESS_KEY_ID, LOADER_AWS_SECRET_ACCESS_KEY, LOADER_AWS_REGION, S3_LANDING_BUCKET.

## Data contract (raw files exactly as generated -- no anomaly labels, Gold must detect them)
- suppliers.csv: supplier_id,supplier_name,contact_email,country,currency,payment_terms,bank_account,is_active,created_at,updated_at
  (~15% of suppliers appear twice with changed payment_terms/bank_account and later updated_at = SCD2 source; is_active written as True/False)
- purchase_orders.csv: po_id,supplier_id,quantity,unit_price,line_total,currency,po_date,expected_delivery_date,status,ingested_at
  (status OPEN/CLOSED/CANCELLED)
- receipts.csv: receipt_id,po_id,supplier_id,quantity_received,receipt_date,received_by,ingested_at
- invoices.csv: invoice_id,supplier_id,po_id,invoice_amount,po_currency,invoice_currency,invoice_date,due_date,status,ingested_at
  (po_id and po_currency are EMPTY STRINGS when missing -- COPY INTO needs EMPTY_FIELD_AS_NULL / NULL_IF handling; ~3% re-submitted duplicate rows)
- payments.jsonl (local only until Phase 5): payment_id,invoice_id,supplier_id,amount_paid,invoice_amount,currency,payment_date,
  payment_method,tranche,event_timestamp,updated_at (partial payments = 2 events, tranche 1 and 2, same invoice_id)
- Timestamps are ISO-8601 with timezone (e.g. 2026-08-22T13:04:16+00:00); dates are YYYY-MM-DD.
- Anomalies injected (all unlabelled): receipt qty != PO qty, invoice amount != PO line_total, invoice currency != PO currency,
  invoice with no PO, invoice against a CANCELLED PO, duplicate invoice submissions, late payments, partial payments, supplier SCD2 changes.
- Generator: `python scripts/generate_raw_data.py [--local-only] [--seed N] [--num-pos N ...]`. Tests: `pytest` (24 pass).

## Key design decisions (with the "why" -- be ready to explain in interviews)
- One S3 bucket, dataset-first prefixes: one prefix = one stage = one scoped IAM policy; fewer policy surfaces to keep in sync.
- Separate admin and service identities in AWS and Snowflake: blast-radius limitation (same principle as the resource monitor).
- Raw data carries no anomaly flags; detection happens in Gold. Bronze is thin/raw, Silver cleans/dedupes/MERGEs, Gold holds business logic.
- Payments MERGE key = payment_id with updated_at comparison (upsert), because Kafka is at-least-once.
- dbt test severity rule: `error` for anything that would make finance distrust the reconciliation; `warn` for cosmetic issues.
- Airflow: retries (about 2, with backoff) for transient task failures; consumer health check should alert fast with ~0 retries.
- Snowflake dev/CI/prod = separate DATABASES in one account (FINANCE_DEV now; FINANCE_CI and FINANCE_PROD in Phase 11).
- Terraform splits provider roles: SYSADMIN creates objects, SECURITYADMIN creates roles/grants. ACCOUNTADMIN is never held by automation.

## Next steps (in order)
1. Finish Phase 4: a LOADER service user (key-pair, default role LOADER_ROLE, default warehouse LOADING_WH, TIMEZONE='UTC') so
   COPY INTO runs as a service identity, not Narendra's login. Needed before Airflow (Phase 10) can run sql/04_load_raw.sql.
2. Phase 5 (MSK Serverless): create per session and DELETE (not stop) at the end -- no auto-suspend. IAM auth only; no public broker for real use.
   Write consumer tests for "same payment delivered twice -> one row". Add UPDATE to LOADER_ROLE's RAW table grants for the MERGE.
3. Later: masking policy on suppliers.bank_account for REPORTER_ROLE (Enterprise feature) once the Silver/Gold supplier model exists.

## Open items
- Check the Snowflake activation email was clicked.
- Stray file `Good.docx` in the project root (gitignored, harmless).
- Remote Terraform state, second environment, encrypted key in a secrets manager = "what I'd change for production" (README Status section).

## Lessons / interview stories (real things that happened)
- A saved Terraform plan (`tfplan`) got committed; checked it for secrets (none), untracked it, gitignored plan files and keys.
- Rotated the loader AWS key with `terraform apply -replace=...` (2026-11-16). Old key was destroyed before .env had the new one = brief outage;
  production fix is zero-downtime rotation (second key -> deploy -> delete old) or no long-lived keys at all (IAM role). Wrote the new key into
  .env straight from `terraform output` with sed (no clipboard), verified by a real S3 upload.
- A stray `%` (zsh end-of-output marker) copied into a secret key caused AWS SignatureDoesNotMatch. Use `| pbcopy` instead of hand-copying.
- A second `required_providers` block in another file made `terraform init` fail; Terraform allows exactly one per module.
- Seeding the generator exposed two reproducibility traps: uuid4 is not seedable, and iterating a set of strings varies with PYTHONHASHSEED.
- The old Snowflake provider (`snowflake-labs`, 0.x) is outdated; current is `snowflakedb/snowflake` 2.x.
- Copied grant syntax from the Pulumi docs (`on_schema = { ... }`) -> `terraform validate` failed: in HCL those are nested BLOCKS
  (`on_schema { ... }`), not map attributes. Validate before trusting any snippet.
- Negative RBAC test: CREATE TABLE as REPORTER_ROLE fails as designed. Did NOT click Snowsight's "Resolve access error" (it would grant the privilege).

- Pulumi docs mirror the provider version but NOT HCL names: describe_outputs -> describe_output, null_ifs -> null_if. Validate catches it.
- First COPY INTO written by Narendra himself: caught stage-name typo, missing ';', SUUPLIERS typo and $1-$10 on the 7-column receipts file
  (column-count mismatch). DESC TABLE before writing a load is the habit. Idempotency shown live (0 files processed on re-run).
- COPY idempotency is FILE-level (path + checksum, per table, 64 days): changed content or a renamed copy WILL reload. Row-level
  idempotency is a modelling job (Silver dedupe; MERGE on payment_id for streaming).

## How to work with me (Narendra)
- I want exact commands and copy-paste code and clear "do this next" steps, not a stream of questions. I am short on patience for long setup.
- Explain concepts as: simple analogy -> why it matters in production -> practical takeaway. I am preparing for DE interviews and want
  lead-level reasoning, plus "how would I say this in an interview".
- Tell me where to run things (VS Code terminal vs Snowflake worksheet vs AWS console). Never ask me to paste secrets; I redact keys in screenshots.
- Flag scope creep (PySpark, Hadoop, Scala) before helping with it. Be honest about what is simplified versus production-grade.
- Run git write commands from my terminal, not from a sandbox (stale lock files). Commit with explicit `git add <paths>`, never `git add .`.

## Routine loops
- Save work: `git status` -> `git add <files>` -> `git commit -m` -> `git push origin main`
- Check code: `pip install -r requirements-dev.txt` -> `pytest`
- Change infra: `terraform init` -> `terraform plan` -> `terraform apply`  (run from `infra/`)
