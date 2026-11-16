-- =====================================================================================
-- 00 BOOTSTRAP -- run ONCE, by a human, as ACCOUNTADMIN.
-- Everything else in this project is Terraform (as TERRAFORM_SVC with SYSADMIN / SECURITYADMIN).
-- These are the only steps that need the "master key", so they are done by hand and recorded here.
-- Earlier bootstrap steps (TERRAFORM_SVC user + key pair, ACCOUNT_GUARD resource monitor) are described in PROJECT_STATUS.md.
-- =====================================================================================

-- Phase 4 (2026-11-16): let SYSADMIN create the S3 storage integration (FINANCE_S3_INT) via Terraform.
USE ROLE ACCOUNTADMIN;
GRANT CREATE INTEGRATION ON ACCOUNT TO ROLE SYSADMIN;
USE ROLE SYSADMIN;
