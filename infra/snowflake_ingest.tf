# Phase 4 -- S3 -> Snowflake trust and external stages.
#
# How Snowflake reads S3 without any AWS keys:
#   1. Snowflake's storage integration has its own AWS IAM user (owned by Snowflake) and an external ID.
#   2. We create an IAM role in OUR account that trusts ONLY that user + external ID, with read-only S3 access.
#   3. At COPY time Snowflake assumes the role and gets short-lived credentials. Nothing to rotate, nothing to leak.
#
# Breaking the circular dependency (most tutorials need two applies + copy/paste from DESC INTEGRATION):
#   - The integration needs the role's ARN.  The role's trust policy needs the integration's IAM user + external ID.
#   - An IAM role ARN is deterministic (arn:aws:iam::<account>:role/<name>), so we BUILD it as a string.
#   - The integration then depends on a string, not on the role resource -> no cycle -> one apply.
#
# Provider notes (snowflakedb/snowflake 2.23): snowflake_storage_integration_aws and snowflake_stage_external_s3
# are the newer, purpose-built resources and are NOT preview features.
# Bootstrap: SYSADMIN was granted CREATE INTEGRATION ON ACCOUNT manually by ACCOUNTADMIN (2026-11-16).

data "aws_caller_identity" "current" {}

locals {
  bucket_name = aws_s3_bucket.finance_pipeline_dublin.bucket
  bucket_arn  = aws_s3_bucket.finance_pipeline_dublin.arn

  snowflake_s3_role_name = "finance-pipeline-snowflake-s3-read"
  snowflake_s3_role_arn  = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${local.snowflake_s3_role_name}"

  # One external stage per dataset prefix (dataset-first S3 layout). Payments arrive via Kafka in Phase 5, not S3.
  batch_datasets = {
    SUPPLIERS       = "suppliers"
    PURCHASE_ORDERS = "purchase_orders"
    RECEIPTS        = "receipts"
    INVOICES        = "invoices"
  }
}

# --- Snowflake side: storage integration (account-level object, owned by SYSADMIN) ----------------

resource "snowflake_storage_integration_aws" "s3" {
  name             = "FINANCE_S3_INT"
  comment          = "Read-only access to the finance-pipeline landing bucket via an assumed IAM role"
  enabled          = true
  storage_provider = "S3"

  storage_aws_role_arn = local.snowflake_s3_role_arn

  # Allow-list: stages using this integration can only point inside this bucket.
  storage_allowed_locations = ["s3://${local.bucket_name}/"]
}

# --- AWS side: the role Snowflake assumes ---------------------------------------------------------

resource "aws_iam_role" "snowflake_s3_read" {
  name        = local.snowflake_s3_role_name
  description = "Assumed by Snowflake storage integration FINANCE_S3_INT to read the landing bucket"

  # Trust ONLY Snowflake's IAM user for this integration, and ONLY with our external ID.
  # The external ID prevents the "confused deputy" problem: another Snowflake customer can't point their
  # integration at our role ARN and read our bucket, because they don't have our external ID.
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { AWS = snowflake_storage_integration_aws.s3.describe_output[0].iam_user_arn }
        Action    = "sts:AssumeRole"
        Condition = {
          StringEquals = { "sts:ExternalId" = snowflake_storage_integration_aws.s3.describe_output[0].external_id }
        }
      }
    ]
  })
}

# Read-only: Snowflake loads files, it never writes or deletes them. (No PURGE = TRUE on COPY.)
resource "aws_iam_role_policy" "snowflake_s3_read" {
  name = "snowflake-s3-read-only"
  role = aws_iam_role.snowflake_s3_read.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:GetObjectVersion"]
        Resource = "${local.bucket_arn}/*"
      },
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:GetBucketLocation"]
        Resource = local.bucket_arn
      }
    ]
  })
}

# --- External stages in RAW, one per dataset prefix (owned by SYSADMIN) ---------------------------

resource "snowflake_stage_external_s3" "batch" {
  for_each = local.batch_datasets

  name                = "${each.key}_STAGE"
  database            = snowflake_database.this.name
  schema              = snowflake_schema.this["RAW"].name
  url                 = "s3://${local.bucket_name}/${each.value}/"
  storage_integration = snowflake_storage_integration_aws.s3.name
  comment             = "S3 prefix ${each.value}/ -> RAW.${each.key}"

  # CSV parsing rules live with the stage, so every COPY INTO from it reads files the same way.
  file_format {
    csv {
      skip_header                  = 1
      field_optionally_enclosed_by = "\""
      # Invoices write missing po_id / po_currency as empty strings -> load them as NULL, not ''.
      null_if  = [""]
    }
  }

  # The role must exist before anyone LISTs or COPYs from the stage.
  depends_on = [aws_iam_role_policy.snowflake_s3_read]
}

# LOADER_ROLE needs USAGE on a stage to COPY INTO from it. Nobody else gets stage access.
resource "snowflake_grant_privileges_to_account_role" "loader_stage_usage" {
  provider = snowflake.security_admin
  for_each = local.batch_datasets

  account_role_name = snowflake_account_role.this["LOADER_ROLE"].name
  privileges        = ["USAGE"]
  on_schema_object {
    object_type = "STAGE"
    object_name = snowflake_stage_external_s3.batch[each.key].fully_qualified_name
  }
}

output "snowflake_stages" {
  value = [for s in snowflake_stage_external_s3.batch : s.fully_qualified_name]
}
