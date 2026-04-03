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
#}

{% macro meticulous_data_start_filter(config_source, date_column='REPORT_DATE') %}

{%- set start_query -%}
    SELECT CONFIG_VALUE
    FROM {{ config_source }}
    WHERE CONFIG_KEY = 'data_start_date'
    LIMIT 1
{%- endset -%}

{%- if execute -%}
    {%- set result = run_query(start_query) -%}
    {%- if result and result.rows | length > 0 and result.rows[0][0] -%}
        AND {{ date_column }} >= '{{ result.rows[0][0] }}'
    {%- endif -%}
{%- endif -%}

{% endmacro %}
