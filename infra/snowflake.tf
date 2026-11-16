# Snowflake provider -- authenticates as the TERRAFORM_SVC service user using a key pair.
#
# Two provider configurations because Snowflake splits duties across roles:
#   SYSADMIN      -> creates objects (warehouses, databases, schemas)
#   SECURITYADMIN -> creates roles and grants (who can access what)
# Resources that manage access will say `provider = snowflake.security_admin`.
# ACCOUNTADMIN is deliberately NOT held by automation.

provider "snowflake" {
  organization_name = var.snowflake_organization_name
  account_name      = var.snowflake_account_name
  user              = "TERRAFORM_SVC"
  authenticator     = "SNOWFLAKE_JWT"
  private_key       = file(pathexpand(var.snowflake_private_key_path))
  role              = "SYSADMIN"

  # snowflake_table is still a PREVIEW resource in 2.23 (needed for the RAW landing tables, Phase 4).
  # Enabled only on the SYSADMIN provider, the only one that creates tables.
  preview_features_enabled = ["snowflake_table_resource"]
}

provider "snowflake" {
  alias             = "security_admin"
  organization_name = var.snowflake_organization_name
  account_name      = var.snowflake_account_name
  user              = "TERRAFORM_SVC"
  authenticator     = "SNOWFLAKE_JWT"
  private_key       = file(pathexpand(var.snowflake_private_key_path))
  role              = "SECURITYADMIN"
}
