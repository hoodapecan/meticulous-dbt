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
{%- set emitted = namespace(any=false) -%}
{%- for field in field_names if field not in skip_fields -%}
    {%- set reserved = ['EVENT', 'TARGET', 'ORDER', 'GROUP', 'SELECT', 'TABLE', 'COLUMN', 'INDEX', 'KEY', 'VALUE', 'COMMENT'] -%}
    {%- set col_name = '"' ~ field | upper ~ '"' if field | upper in reserved else field -%}
    {%- set emitted.any = true -%}
    {%- if alias -%}
        {{ alias }}.{{ col_name }}
    {%- else -%}
        {{ col_name }}
    {%- endif -%}
    {%- if not loop.last %},
    {% endif -%}
{%- endfor -%}

{#-
    Empty-source guard: when the source has zero rows OR every row is in
    skip_fields (e.g. only `platform`), the for-loop emits nothing. The
    callers all wrap this macro between commas in a SELECT list, so an
    empty output produces ", , " and a syntax error. Emit a single null-
    valued placeholder column so the surrounding commas resolve cleanly.
    Disappears the moment the taxonomy mappings table has classified data.
-#}
{%- if not emitted.any -%}
    null as _meticulous_no_taxonomy_yet
{%- endif -%}

{% endmacro %}
