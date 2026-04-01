{#
    meticulous_taxonomy_columns

    Generates a comma-separated list of taxonomy dimension columns
    with an optional table alias prefix. Discovers columns dynamically
    from the METICULOUS_TAXONOMY_MAPPINGS table.

    Usage:
        select
            u.platform,
            u.campaign_id,
            {{ meticulous_dbt.meticulous_taxonomy_columns(
                source('meticulous', 'meticulous_taxonomy_mappings'),
                alias='t'
            ) }}
        from unioned u
        left join taxonomy t on ...

    Output:
        t.brand,
        t.channel,
        t.tactic,
        ...
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

{%- for field in field_names -%}
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
