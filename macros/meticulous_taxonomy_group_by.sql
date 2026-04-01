{#
    meticulous_taxonomy_group_by

    Returns the number of taxonomy dimension columns.
    Use with dbt's group-by-number pattern.

    Usage:
        -- If your select has 4 non-taxonomy columns before the taxonomy columns:
        group by 1, 2, 3, 4
            {{ meticulous_dbt.meticulous_taxonomy_group_by(
                source('meticulous', 'meticulous_taxonomy_mappings'),
                offset=4
            ) }}

    Output (if 8 taxonomy fields):
        , 5, 6, 7, 8, 9, 10, 11, 12
#}

{% macro meticulous_taxonomy_group_by(taxonomy_source, offset=0) %}

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

{%- for field in field_names -%}
    , {{ offset + loop.index }}
{%- endfor -%}

{% endmacro %}
