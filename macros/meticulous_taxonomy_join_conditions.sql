{#
    meticulous_taxonomy_join_conditions

    Generates JOIN conditions for budget pacing: each taxonomy dimension
    matches if the budget filter is null OR equals the spend dimension.

    Usage in meticulous__budget_pacing.sql:

        left join actual_spend s
            on bd.report_date = s.report_date
            {{ meticulous_dbt.meticulous_taxonomy_join_conditions(
                source('meticulous', 'meticulous_taxonomy_mappings'),
                left_alias='bd',
                right_alias='s'
            ) }}

    Output:
        and (bd.brand is null or lower(bd.brand) = lower(s.brand))
        and (bd.channel is null or lower(bd.channel) = lower(s.channel))
        ...
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

{%- for field in field_names -%}
    {%- set col_name = '"EVENT"' if field | upper == 'EVENT' else field %}
            and ({{ left_alias }}.{{ col_name }} is null or lower({{ left_alias }}.{{ col_name }}) = lower({{ right_alias }}.{{ col_name }}))
{%- endfor -%}

{% endmacro %}
