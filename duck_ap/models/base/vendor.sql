{{
    config(
        materialized='view'
    )
}}

with source as (

    select * from {{ ref('ap_vendor') }}

)

select
    vendor_id::varchar                  as vendor_key,
    vendor_name::varchar                as vendor_name,
    concat_ws(', ',
        nullif(trim(remit_address_line1), ''),
        nullif(trim(remit_city), ''),
        nullif(trim(remit_state), ''),
        nullif(trim(remit_zip::varchar), '')
    )::varchar                          as remit_address,
    vendor_type::varchar                as vendor_type,
    (vendor_status = 'ACTIVE')::boolean as active_status

from source
