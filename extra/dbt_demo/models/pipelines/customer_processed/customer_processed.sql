with source as (
    select * from {{ ref('customers') }}

),

processing as (

    select
        customer_id as new_customer_id,
        last_name as new_last_name

    from source

)

select * from processing
