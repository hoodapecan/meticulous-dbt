{#
    meticulous_taxonomy_join_conditions

    Generates null-safe JOIN conditions for budget pacing.

    Usage:
        {{ meticulous_dbt.meticulous_taxonomy_join_conditions(
            source('meticulous', 'meticulous_taxonomy_mappings'),
            left_alias='bd',
            right_alias='s'
        ) }}
#}

{% macro meticulous_taxonomy_join_conditions(taxonomy_source, left_alias, right_alias) %}

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
    {%- set reserved = ['EVENT', 'TARGET', 'ORDER', 'GROUP', 'SELECT', 'TABLE', 'COLUMN', 'INDEX', 'KEY', 'VALUE', 'COMMENT'] -%}
    {%- set col_name = '"' ~ field | upper ~ '"' if field | upper in reserved else field %}
            and ({{ left_alias }}.{{ col_name }} is null or lower({{ left_alias }}.{{ col_name }}) = lower({{ right_alias }}.{{ col_name }}))
{%- endfor -%}

{% endmacro %}
