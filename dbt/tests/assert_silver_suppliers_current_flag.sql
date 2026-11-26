-- SCD2 consistency: is_current must agree with valid_to.
-- valid_to IS NULL <=> is_current = TRUE. A row with an end date marked current
-- (or an open row marked historical) means the table contradicts itself. Zero rows = pass.


select * 
from {{ ref('silver_suppliers') }}
where (valid_to is null) != is_current