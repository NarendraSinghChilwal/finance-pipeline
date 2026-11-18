
select
    supplier_id,
    supplier_name,
    contact_email,
    country,
    currency,
    payment_terms,
    bank_account,
    try_cast(is_active as boolean)         as is_active,
    try_cast(created_at as timestamp_tz)   as created_at,
    try_cast(updated_at as timestamp_tz)   as updated_at,
    _source_file,
    _source_row_number,
    _loaded_at
from {{ source('raw', 'suppliers') }}