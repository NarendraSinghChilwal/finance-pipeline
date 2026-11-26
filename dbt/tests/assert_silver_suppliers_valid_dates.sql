-- SCD2 date sanity: a closed version cannot end before (or at the moment) it starts.
-- Current versions have valid_to = NULL (still open), so they are excluded.
-- Zero rows returned = pass.



select 
    *
from {{ ref('silver_suppliers') }}
where valid_to is not null and valid_from >= valid_to