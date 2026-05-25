{#
    meticulous_data_start_filter

    Returns a SQL WHERE clause fragment that filters by data_start_date
    from the METICULOUS_CONFIG table. If the config table doesn't exist
    or has no data_start_date, returns nothing (no filter applied).

    Usage:
        WHERE metric_name IS NOT NULL
            {{ meticulous_dbt.meticulous_data_start_filter(
                source('meticulous', 'meticulous_config'),
                date_column='calendardate'
            ) }}

    CI / lint:
        The macro runs a query at compile time. To skip the lookup (e.g. in
        a CI lint step that points at a stub no-connection profile), set
        EITHER:
          - env var METICULOUS_SKIP_RUNTIME_LOOKUPS=true (preferred — reliable
            in any caller, including sqlfluff lint with the dbt templater)
          - or dbt var skip_meticulous_runtime_lookups=true
        With either set, no filter is emitted.
#}

{% macro meticulous_data_start_filter(config_source, date_column='REPORT_DATE') %}

{%- set start_query -%}
    SELECT CONFIG_VALUE
    FROM {{ config_source }}
    WHERE CONFIG_KEY = 'data_start_date'
    LIMIT 1
{%- endset -%}

{%- if execute and not meticulous_dbt._skip_runtime_lookups() -%}
    {%- set result = run_query(start_query) -%}
    {%- if result and result.rows | length > 0 and result.rows[0][0] -%}
        AND {{ date_column }} >= '{{ result.rows[0][0] }}'
    {%- endif -%}
{%- endif -%}

{% endmacro %}
