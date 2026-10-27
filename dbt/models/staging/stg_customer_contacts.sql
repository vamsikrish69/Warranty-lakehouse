-- Staging model for customer_contacts.
-- 1) parse the raw JSON string into typed columns
-- 2) drop rows with no key, keep ONE row per contact_id (the latest version)
-- 3) flag (do not delete) suspicious values so later layers can decide
-- NOTE: warranty_status_at_contact is what the contact system CLAIMED at the time.
--       Keep it raw; int_warranty_validation compares it with the date-derived status.

with parsed as (

    select
        value:contact_id::string                  as contact_id,
        value:customer_id::string                 as customer_id,
        value:asset_id::string                    as asset_id,
        value:channel::string                     as channel,
        value:issue_category::string              as issue_category,
        value:duration_sec::int                   as duration_sec,
        value:agent_id::string                    as agent_id,
        value:warranty_status_at_contact::string  as warranty_status_at_contact,
        value:troubleshooting_outcome::string     as troubleshooting_outcome,
        value:first_contact_resolved::boolean     as first_contact_resolved,
        value:event_ts::timestamp                 as event_ts,
        value:updated_at::timestamp               as updated_at,
        value:batch_id::string                    as batch_id,
        _source_file,
        _loaded_at
    from {{ source('bronze', 'customer_contacts') }}

),

latest as (

    select *
    from parsed
    where contact_id is not null
    qualify row_number() over (
        partition by contact_id
        order by updated_at desc, _loaded_at desc
    ) = 1

)

select
    *,
    coalesce(
        channel not in ('VOICE_SUPPORT', 'CHAT_SUPPORT', 'FACEBOOK_SUPPORT', 'SOCIAL_MEDIA_SUPPORT'),
        false
    ) as is_invalid_channel
from latest