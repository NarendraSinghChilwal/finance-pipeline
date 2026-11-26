-- Silver suppliers: SCD Type 2.
-- Grain: one row per supplier per version. Every Bronze row is kept (nothing dropped);
-- valid_from/valid_to give the period each version was true, is_current marks the latest.
-- History comes from the source itself (both versions arrive in the same file), so it is
-- derived here with LEAD() rather than a dbt snapshot, which would fail on duplicate keys.

select
    supplier_id,
    supplier_name,
    contact_email,
    country,
    currency,
    payment_terms,
    bank_account,
    is_active,
    created_at,                     -- when the supplier was first set up; same on every version
    updated_at,

    -- A version is valid from the moment it was written...
    updated_at as valid_from,

    -- ...until the next version of the same supplier was written. NULL = no next version yet.
    -- _source_row_number is a tiebreaker so the order is deterministic if two versions share updated_at.
    lead(updated_at) over (partition by supplier_id order by updated_at, _source_row_number) as valid_to,

    -- Current version = the one with no next version.
    -- Could also be written as (valid_to is null as is_current); kept explicit for readability.
    lead(updated_at) over (partition by supplier_id order by updated_at, _source_row_number) is null as is_current,

    _source_file,
    _source_row_number,
    _loaded_at
from {{ ref('bronze_suppliers') }}