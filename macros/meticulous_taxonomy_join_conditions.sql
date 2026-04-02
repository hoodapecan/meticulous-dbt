{#
    meticulous_taxonomy_join_conditions

    Generates null-safe JOIN conditions for budget pacing.
    Discovers columns from the pivoted staging model via INFORMATION_SCHEMA.

    Usage:
        left join actual_spend s
            on bd.report_date = s.report_date
            {{ meticulous_dbt.meticulous_taxonomy_join_conditions(
                ref('stg_meticulous__taxonomy_mappings'),
                left_alias='bd',
                right_alias='s'
            ) }}
#}

{% macro meticulous_taxonomy_join_conditions(taxonomy_relation, left_alias, right_alias) %}

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
    {%- set col_name = '"EVENT"' if col | upper == 'EVENT' else col %}
            and ({{ left_alias }}.{{ col_name }} is null or lower({{ left_alias }}.{{ col_name }}) = lower({{ right_alias }}.{{ col_name }}))
{%- endfor -%}

{% endmacro %}
