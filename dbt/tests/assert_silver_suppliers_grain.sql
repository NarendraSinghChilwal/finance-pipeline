-- SCD2 grain: one row per supplier per version, identified by (supplier_id, valid_from).
-- supplier_id alone is NOT unique (one row per version), but no supplier may have two
-- versions starting at the same moment. Zero rows returned = pass.



select 
    supplier_id,
    valid_from,
    count(*) as version_count
from {{ ref('silver_suppliers') }}
group by 1,2
having count(*) > 1