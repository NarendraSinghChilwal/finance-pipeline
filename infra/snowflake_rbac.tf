# Phase 2 -- Snowflake RBAC. Everything here uses provider = snowflake.security_admin (role SECURITYADMIN):
# SECURITYADMIN can CREATE ROLE and holds MANAGE GRANTS, so it can grant on SYSADMIN-owned objects
# without owning them. Object creation (SYSADMIN) and access control (SECURITYADMIN) stay separated.
#
# Access model (least privilege, one role per workload):
#   LOADER_ROLE      -> LOADING_WH;      write RAW only
#   TRANSFORMER_ROLE -> TRANSFORMING_WH; read RAW, build BRONZE / SILVER / GOLD
#   REPORTER_ROLE    -> REPORTING_WH;    read GOLD only

locals {
  roles = {
    LOADER_ROLE      = "Loads raw data into RAW (COPY INTO, Kafka consumer). No access to modelled layers."
    TRANSFORMER_ROLE = "dbt. Reads RAW, builds BRONZE/SILVER/GOLD."
    REPORTER_ROLE    = "Read-only access to GOLD for reporting."
  }

  warehouse_role = {
    LOADING_WH      = "LOADER_ROLE"
    TRANSFORMING_WH = "TRANSFORMER_ROLE"
    REPORTING_WH    = "REPORTER_ROLE"
  }

  # Schema-level privileges, keyed "ROLE.SCHEMA".
  schema_grants = {
    "LOADER_ROLE.RAW"         = { role = "LOADER_ROLE", schema = "RAW", privileges = ["USAGE"] }
    "TRANSFORMER_ROLE.RAW"    = { role = "TRANSFORMER_ROLE", schema = "RAW", privileges = ["USAGE"] }
    "TRANSFORMER_ROLE.BRONZE" = { role = "TRANSFORMER_ROLE", schema = "BRONZE", privileges = ["USAGE", "CREATE TABLE", "CREATE VIEW"] }
    "TRANSFORMER_ROLE.SILVER" = { role = "TRANSFORMER_ROLE", schema = "SILVER", privileges = ["USAGE", "CREATE TABLE", "CREATE VIEW"] }
    "TRANSFORMER_ROLE.GOLD"   = { role = "TRANSFORMER_ROLE", schema = "GOLD", privileges = ["USAGE", "CREATE TABLE", "CREATE VIEW"] }
    "REPORTER_ROLE.GOLD"      = { role = "REPORTER_ROLE", schema = "GOLD", privileges = ["USAGE"] }
  }

  # Object-level privileges, keyed "ROLE.SCHEMA.TYPE". Each entry becomes TWO grants (ALL + FUTURE), see below.
  # LOADER gets INSERT + SELECT for COPY INTO. Phase 5 (Kafka MERGE on payment_id) will add UPDATE.
  object_grants = {
    "LOADER_ROLE.RAW.TABLES"      = { role = "LOADER_ROLE", schema = "RAW", type = "TABLES", privileges = ["INSERT", "SELECT"] }
    "TRANSFORMER_ROLE.RAW.TABLES" = { role = "TRANSFORMER_ROLE", schema = "RAW", type = "TABLES", privileges = ["SELECT"] }
    "REPORTER_ROLE.GOLD.TABLES"   = { role = "REPORTER_ROLE", schema = "GOLD", type = "TABLES", privileges = ["SELECT"] }
    "REPORTER_ROLE.GOLD.VIEWS"    = { role = "REPORTER_ROLE", schema = "GOLD", type = "VIEWS", privileges = ["SELECT"] }
  }
}

# --- Roles ---------------------------------------------------------------------------------------

resource "snowflake_account_role" "this" {
  provider = snowflake.security_admin
  for_each = local.roles

  name    = each.key
  comment = each.value
}

# Snowflake best practice: every custom role rolls up to SYSADMIN. Otherwise tables that dbt
# (TRANSFORMER_ROLE) creates would be invisible to the admins -- "orphaned" objects.
resource "snowflake_grant_account_role" "to_sysadmin" {
  provider = snowflake.security_admin
  for_each = local.roles

  role_name        = snowflake_account_role.this[each.key].name
  parent_role_name = "SYSADMIN"
}

# --- Account-object grants (warehouse, database) -------------------------------------------------

resource "snowflake_grant_privileges_to_account_role" "warehouse_usage" {
  provider = snowflake.security_admin
  for_each = local.warehouse_role

  account_role_name = snowflake_account_role.this[each.value].name
  privileges        = ["USAGE"] # USAGE is enough to run queries and to trigger auto-resume
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.this[each.key].name
  }
}

resource "snowflake_grant_privileges_to_account_role" "database_usage" {
  provider = snowflake.security_admin
  for_each = local.roles

  account_role_name = snowflake_account_role.this[each.key].name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.this.name
  }
}

# --- Schema grants -------------------------------------------------------------------------------

resource "snowflake_grant_privileges_to_account_role" "schema" {
  provider = snowflake.security_admin
  for_each = local.schema_grants

  account_role_name = snowflake_account_role.this[each.value.role].name
  privileges        = each.value.privileges
  on_schema {
    schema_name = snowflake_schema.this[each.value.schema].fully_qualified_name
  }
}

# --- Object grants: ALL (existing objects) + FUTURE (objects created later) ----------------------
# Neither covers the other: FUTURE never backfills existing tables, ALL never covers new ones.
# Granted at SCHEMA level on purpose -- schema-level future grants override database-level ones.

resource "snowflake_grant_privileges_to_account_role" "objects_all" {
  provider = snowflake.security_admin
  for_each = local.object_grants

  account_role_name = snowflake_account_role.this[each.value.role].name
  privileges        = each.value.privileges
  on_schema_object {
    all {
      object_type_plural = each.value.type
      in_schema          = snowflake_schema.this[each.value.schema].fully_qualified_name
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "objects_future" {
  provider = snowflake.security_admin
  for_each = local.object_grants

  account_role_name = snowflake_account_role.this[each.value.role].name
  privileges        = each.value.privileges
  on_schema_object {
    future {
      object_type_plural = each.value.type
      in_schema          = snowflake_schema.this[each.value.schema].fully_qualified_name
    }
  }
}

# --- Outputs -------------------------------------------------------------------------------------

output "snowflake_database" {
  value = snowflake_database.this.name
}

output "snowflake_schemas" {
  value = [for s in snowflake_schema.this : s.fully_qualified_name]
}

output "snowflake_roles" {
  value = [for r in snowflake_account_role.this : r.name]
}
