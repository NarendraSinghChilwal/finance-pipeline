-- =====================================================================================
-- 04 LOAD RAW -- batch load S3 -> FINANCE_DEV.RAW with COPY INTO.
-- Run as LOADER_ROLE (least privilege: it can only write RAW and use the 4 stages).
-- Safe to re-run: Snowflake keeps 64 days of load history per table and skips files already loaded.
-- Phase 10: Airflow runs these same statements on a schedule.
-- =====================================================================================

USE ROLE LOADER_ROLE;
USE WAREHOUSE LOADING_WH;
USE SCHEMA FINANCE_DEV.RAW;

-- 0. Sanity check: what files are waiting in S3? ---------------------------------------
LIST @INVOICES_STAGE;

-- 1. Load -------------------------------------------------------------------------------
-- $1..$n are CSV columns by position (header order), then 3 lineage columns.
COPY INTO SUPPLIERS FROM (
  SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()
  FROM @SUPPLIERS_STAGE);

COPY INTO PURCHASE_ORDERS FROM (
  SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()
  FROM @PURCHASE_ORDERS_STAGE);

COPY INTO RECEIPTS FROM (
  SELECT $1,$2,$3,$4,$5,$6,$7, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()
  FROM @RECEIPTS_STAGE);

COPY INTO INVOICES FROM (
  SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()
  FROM @INVOICES_STAGE);

-- 2. Verify: rows per table per file -----------------------------------------------------
-- Expected per run: suppliers 238, purchase_orders 5000, receipts ~3344, invoices 6180.
SELECT 'SUPPLIERS' AS tbl, _source_file, COUNT(*) AS rows_loaded FROM SUPPLIERS GROUP BY 1,2
UNION ALL SELECT 'PURCHASE_ORDERS', _source_file, COUNT(*) FROM PURCHASE_ORDERS GROUP BY 1,2
UNION ALL SELECT 'RECEIPTS',        _source_file, COUNT(*) FROM RECEIPTS        GROUP BY 1,2
UNION ALL SELECT 'INVOICES',        _source_file, COUNT(*) FROM INVOICES        GROUP BY 1,2
ORDER BY 1,2;

-- 3. Idempotency test: re-run section 1. Every COPY should report "0 files processed".
