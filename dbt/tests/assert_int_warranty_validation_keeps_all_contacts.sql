-- Fails (returns a row) if the join added or dropped contacts.
select *
from (
    select
        (select count(*) from {{ ref('int_warranty_validation') }}) as int_rows,
        (select count(*) from {{ ref('stg_customer_contacts') }})   as stg_rows
)
where int_rows <> stg_rows