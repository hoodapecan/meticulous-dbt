{#
    meticulous_taxonomy_pivot

    Generates a staging model that pivots METICULOUS_TAXONOMY_MAPPINGS
    from vertical (one row per field) to wide (one row per campaign).

    Discovers field_name values at compile time — no hardcoding needed.

    Usage in stg_meticulous__taxonomy_mappings.sql:

        {{ meticulous_dbt.meticulous_taxonomy_pivot(
            source('meticulous', 'meticulous_taxonomy_mappings')
        ) }}
#}

{% macro meticulous_taxonomy_pivot(taxonomy_source) %}

{#- Discover distinct field_name values at compile time -#}
{%- set field_query -%}
    select distinct field_name
    from {{ taxonomy_source }}
    where level = 'campaign'
    order by field_name
{%- endset -%}

{%- set results = run_query(field_query) -%}

{%- if execute -%}
    {%- set field_names = results.columns[0].values() -%}
{%- else -%}
    {%- set field_names = [] -%}
{%- endif -%}

with raw as (
    select * from {{ taxonomy_source }}
),

pivoted as (
    select
        platform,
        platform_entity_id as campaign_id,
        entity_name as campaign_name,
        level,
        {%- for field in field_names %}
        max(case when field_name = '{{ field }}' then value_code end) as {{ adapter.quote(field) if field | upper in ['EVENT'] else field }}
        {%- if not loop.last %},{% endif %}
        {%- endfor %}
        {%- if field_names | length > 0 %},{% endif %}
        max(mapped_by) as mapped_by,
        max(mapped_at) as mapped_at
    from raw
    where level = 'campaign'
    group by 1, 2, 3, 4
)

select * from pivoted

{% endmacro %}
