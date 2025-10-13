{{
    config(
        materialized = 'table',
        on_table_exists = 'append'
    )
}}

{# /* Run macro directly to create + insert RCC audit results */ #}
{% do audit_rcc_codes(50) %}
