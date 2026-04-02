{#
    meticulous_taxonomy_columns

    Generates a comma-separated list of taxonomy dimension columns.
    Discovers columns from the pivoted staging model via INFORMATION_SCHEMA.

    Usage:
        {{ meticulous_dbt.meticulous_taxonomy_columns(
            ref('stg_meticulous__taxonomy_mappings'),
            alias='t'
        ) }}
#}

{% macro meticulous_taxonomy_columns(taxonomy_relation, alias=none) %}

{%- set col_query -%}
    select column_name
    from {{ taxonomy_relation.database }}.information_schema.columns
    where table_schema = '{{ taxonomy_relation.schema }}'
      and table_name = '{{ taxonomy_relation.identifier }}'
      and column_name not in ('PLATFORM', 'CAMPAIGN_ID', 'CAMPAIGN_NAME', 'LEVEL', 'MAPPED_BY', 'MAPPED_AT')
    order by ordinal_position
{%- endset -%}

{%- set results = run_query(col_query) -%}

{%- if execute -%}
    {%- set col_names = results.columns[0].values() -%}
{%- else -%}
    {%- set col_names = [] -%}
{%- endif -%}

{%- for col in col_names -%}
    {%- set col_name = '"EVENT"' if col | upper == 'EVENT' else col -%}
    {%- if alias -%}
        {{ alias }}.{{ col_name }}
    {%- else -%}
        {{ col_name }}
    {%- endif -%}
    {%- if not loop.last %},
    {% endif -%}
{%- endfor -%}

{% endmacro %}
