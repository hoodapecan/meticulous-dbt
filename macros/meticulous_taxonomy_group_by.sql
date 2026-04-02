{#
    meticulous_taxonomy_group_by

    Returns numbered group-by references for taxonomy columns.

    Usage:
        group by 1, 2, 3
            {{ meticulous_dbt.meticulous_taxonomy_group_by(
                source('meticulous', 'meticulous_taxonomy_mappings'),
                offset=3
            ) }}
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

{%- set skip_fields = ['platform'] -%}
{%- for field in field_names if field not in skip_fields -%}
    , {{ offset + loop.index }}
{%- endfor -%}

{% endmacro %}
