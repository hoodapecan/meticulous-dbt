{#
    meticulous_taxonomy_pivot

    Generates a staging model that pivots METICULOUS_TAXONOMY_MAPPINGS
    from vertical (one row per field) to wide (one row per entity).

    Two grains supported:
    - level='campaign' (default): one row per (platform, campaign_id),
      pivots fields where taxonomy_mappings.level = 'campaign'.
      Output: platform, campaign_id, campaign_name, level, <fields>, mapped_by, mapped_at.
    - level='ad': one row per (platform, ad_id), pivots fields where
      taxonomy_mappings.level = 'ad'.
      Output: platform, ad_id, ad_name, level, <fields>, mapped_by, mapped_at.

    Discovers field_name values at compile time — no hardcoding needed.
    The campaign-level pivot is backwards compatible (default args).

    Usage in stg_meticulous__taxonomy_mappings.sql:

        {{ meticulous_dbt.meticulous_taxonomy_pivot(
            source('meticulous', 'meticulous_taxonomy_mappings')
        ) }}

    Usage in stg_meticulous__taxonomy_mappings_ad.sql:

        {{ meticulous_dbt.meticulous_taxonomy_pivot(
            source('meticulous', 'meticulous_taxonomy_mappings'),
            level='ad'
        ) }}
#}

{% macro meticulous_taxonomy_pivot(taxonomy_source, level='campaign') %}

{%- set entity_id_alias = level ~ '_id' -%}
{%- set entity_name_alias = level ~ '_name' -%}

{#- Discover distinct field_name values at compile time, scoped to the level -#}
{%- set field_query -%}
    select distinct field_name
    from {{ taxonomy_source }}
    where level = '{{ level }}'
    order by field_name
{%- endset -%}

{%- if execute and not meticulous_dbt._skip_runtime_lookups() -%}
    {%- set results = run_query(field_query) -%}
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
        platform_entity_id as {{ entity_id_alias }},
        entity_name as {{ entity_name_alias }},
        level,
        {%- set skip_fields = ['platform'] -%}
        {%- set reserved = ['EVENT', 'TARGET', 'ORDER', 'GROUP', 'SELECT', 'TABLE', 'COLUMN', 'INDEX', 'KEY', 'VALUE', 'COMMENT'] -%}
        {%- for field in field_names if field not in skip_fields %}
        {#- Quote reserved words (preserve old uppercase behavior) and any
            field containing chars outside [a-z0-9_] (defends against
            historical data where the app didn't normalize field_name). -#}
        {%- set unsafe_chars = modules.re.search('[^a-z0-9_]', field | lower) -%}
        {%- if field | upper in reserved -%}
            {%- set col_name = '"' ~ field | upper ~ '"' -%}
        {%- elif unsafe_chars -%}
            {%- set col_name = '"' ~ field ~ '"' -%}
        {%- else -%}
            {%- set col_name = field -%}
        {%- endif %}
        max(case when field_name = '{{ field }}' then value_code end) as {{ col_name }}
        {%- if not loop.last %},{% endif %}
        {%- endfor %}
        {%- if field_names | length > 0 %},{% endif %}
        max(mapped_by) as mapped_by,
        max(mapped_at) as mapped_at
    from raw
    where level = '{{ level }}'
    group by 1, 2, 3, 4
)

select * from pivoted

{% endmacro %}
