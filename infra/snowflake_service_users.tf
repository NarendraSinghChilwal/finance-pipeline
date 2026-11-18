# Service users -- one Snowflake identity per tool, each locked to one role and one warehouse.
#
#   TERRAFORM_SVC  (bootstrap, created by hand)  -> SYSADMIN / SECURITYADMIN, manages infrastructure
#   LOADER_SVC     (this file)                   -> LOADER_ROLE on LOADING_WH, runs COPY INTO (sql/04_load_raw.sql, later Airflow)
#   DBT_SVC        (this file)                   -> TRANSFORMER_ROLE on TRANSFORMING_WH, runs dbt (Bronze/Silver/Gold)
#
# Why separate users: least privilege per tool, query history shows WHICH tool ran a query, and a leaked key
# is contained (disable one user, nothing else breaks).
#
# TYPE = SERVICE users can't log in with a password at all -- key-pair only. No MFA prompts to break automation.
# Private keys are generated locally with openssl and NEVER leave ~/.snowflake/. Terraform only reads the PUBLIC key.
#
# Created by SECURITYADMIN (inherits USERADMIN -> CREATE USER), same as the roles.

variable "service_user_public_key_paths" {
  type = map(string)
  default = {
    LOADER_SVC = "~/.snowflake/loader_svc_rsa_key.pub"
    DBT_SVC    = "~/.snowflake/dbt_svc_rsa_key.pub"
  }
}

locals {
  service_users = {
    LOADER_SVC = { role = "LOADER_ROLE", warehouse = "LOADING_WH", comment = "Runs COPY INTO from S3 stages into RAW (manual now, Airflow in Phase 10)" }
    DBT_SVC    = { role = "TRANSFORMER_ROLE", warehouse = "TRANSFORMING_WH", comment = "Runs dbt: builds BRONZE / SILVER / GOLD from RAW" }
  }

  # Snowflake wants the public key body only: strip the "-----BEGIN/END PUBLIC KEY-----" lines and join the rest.
  service_user_public_keys = {
    for name, path in var.service_user_public_key_paths :
    name => join("", [for line in split("\n", trimspace(file(pathexpand(path)))) : trimspace(line) if !startswith(trimspace(line), "-----")])
  }
}

resource "snowflake_service_user" "this" {
  provider = snowflake.security_admin
  for_each = local.service_users

  name              = each.key
  comment           = each.value.comment
  default_role      = snowflake_account_role.this[each.value.role].name
  default_warehouse = snowflake_warehouse.this[each.value.warehouse].name
  rsa_public_key    = local.service_user_public_keys[each.key]
}

# A default role is only a preference -- the role must also be GRANTED to the user.
resource "snowflake_grant_account_role" "service_user_role" {
  provider = snowflake.security_admin
  for_each = local.service_users

  role_name = snowflake_account_role.this[each.value.role].name
  user_name = snowflake_service_user.this[each.key].name
}
