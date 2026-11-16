# Phase 2 -- Snowflake objects (compute + storage containers).
# Created by the DEFAULT provider (role SYSADMIN), so SYSADMIN owns every object here.
# Access to these objects is granted in snowflake_rbac.tf by SECURITYADMIN.
#
# Provider: snowflakedb/snowflake 2.23.x. All resources in this file are STABLE (no preview_features_enabled needed).

locals {
  # One warehouse per workload: isolation (a heavy dbt run never queues a loader or a dashboard)
  # and cost attribution (WAREHOUSE_METERING_HISTORY per workload). Idle warehouses cost nothing.
  warehouses = {
    LOADING_WH      = "COPY INTO from S3 and the Kafka consumer MERGE (LOADER_ROLE)"
    TRANSFORMING_WH = "dbt builds Bronze -> Silver -> Gold (TRANSFORMER_ROLE)"
    REPORTING_WH    = "Read-only queries on Gold (REPORTER_ROLE)"
  }

  # Medallion layers. RAW = files exactly as landed; BRONZE thin/typed; SILVER cleaned/deduped/SCD2; GOLD business logic.
  schemas = {
    RAW    = "Landing tables loaded by COPY INTO / Kafka consumer. Written only by LOADER_ROLE."
    BRONZE = "dbt: thin, typed views/tables over RAW."
    SILVER = "dbt: cleaned, deduplicated, incremental models and SCD2 snapshots."
    GOLD   = "dbt: 3-way match and exception detection. Read by REPORTER_ROLE."
  }
}

resource "snowflake_warehouse" "this" {
  for_each = local.warehouses

  name           = each.key
  comment        = each.value
  warehouse_size = "XSMALL" # uppercase avoids perpetual diffs

  auto_suspend        = 60     # seconds; billing is per-second after the first 60s
  auto_resume         = "true" # provider takes a string ("true"/"false") so it can tell "unset" from false
  initially_suspended = true   # create-time only: don't start billing on apply

  # Runaway-query guard. The account resource monitor (ACCOUNT_GUARD) caps the month;
  # this caps a single bad query (e.g. an accidental cross join) at 30 minutes.
  statement_timeout_in_seconds = 1800

  # NOTE: no per-warehouse resource_monitor -- creating monitors needs ACCOUNTADMIN, which automation never holds.
  # ACCOUNT_GUARD (account level, created manually) already covers these warehouses.
}

resource "snowflake_database" "this" {
  name    = var.snowflake_database_name
  comment = "Finance reconciliation pipeline -- ${var.snowflake_database_name}"

  # Time Travel window. Enterprise allows up to 90 days; 1 keeps trial storage cost minimal.
  # Production would raise this on SILVER/GOLD for audit/recovery.
  data_retention_time_in_days = 1

  # We define our own schemas; an unused PUBLIC schema is just an ungoverned place to create objects.
  drop_public_schema_on_creation = true
}

resource "snowflake_schema" "this" {
  for_each = local.schemas

  database = snowflake_database.this.name
  name     = each.key
  comment  = each.value

  # Managed access: only the schema owner (SYSADMIN) or a MANAGE GRANTS holder (SECURITYADMIN) can grant
  # on objects inside. dbt (TRANSFORMER_ROLE) owns the tables it builds, but cannot hand out access to them --
  # all access is decided here, in Terraform. That's the auditability story.
  with_managed_access = "true"
}
