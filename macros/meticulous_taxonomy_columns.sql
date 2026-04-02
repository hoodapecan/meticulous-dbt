{#
    meticulous_taxonomy_columns

    Generates a comma-separated list of taxonomy dimension columns.
    Discovers field names from the source table at compile time.

    Usage:
        {{ meticulous_dbt.meticulous_taxonomy_columns(
            source('meticulous', 'meticulous_taxonomy_mappings'),
            alias='t'
        ) }}
#}

{% macro meticulous_taxonomy_columns(taxonomy_source, alias=none) %}

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

{%- set skip_fields = ['platform'] -%}
{%- for field in field_names if field not in skip_fields -%}
    {%- set col_name = '"EVENT"' if field | upper == 'EVENT' else field -%}
    {%- if alias -%}
        {{ alias }}.{{ col_name }}
    {%- else -%}
        {{ col_name }}
    {%- endif -%}
    {%- if not loop.last %},
    {% endif -%}
{%- endfor -%}

{% endmacro %}
