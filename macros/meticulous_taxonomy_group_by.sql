{#
    meticulous_taxonomy_group_by

    Returns numbered group-by references for taxonomy columns.
    Discovers columns from the pivoted staging model via INFORMATION_SCHEMA.

    Usage:
        group by 1, 2, 3
            {{ meticulous_dbt.meticulous_taxonomy_group_by(
                ref('stg_meticulous__taxonomy_mappings'),
                offset=3
            ) }}
#}

{% macro meticulous_taxonomy_group_by(taxonomy_relation, offset=0) %}

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
    , {{ offset + loop.index }}
{%- endfor -%}

{% endmacro %}
