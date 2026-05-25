{#
    _skip_runtime_lookups (internal helper)

    Returns true if compile-time DB lookups should be skipped — used by
    every macro in this package that calls run_query at compile time.

    Skipped when EITHER:
      - env var METICULOUS_SKIP_RUNTIME_LOOKUPS is truthy (preferred for
        CI / lint contexts — set it on the workflow's env block)
      - dbt var skip_meticulous_runtime_lookups is truthy

    Callers should wrap their run_query AND the result-consumption inside
    `{% if execute and not meticulous_dbt._skip_runtime_lookups() %}`,
    with an `{% else %}` branch that yields a sensible empty fallback
    (usually an empty list of fields).
#}

{% macro _skip_runtime_lookups() %}
    {%- set _via_env = env_var('METICULOUS_SKIP_RUNTIME_LOOKUPS', 'false') | lower in ['true', '1', 'yes'] -%}
    {%- set _via_var = var('skip_meticulous_runtime_lookups', false) in [true, 'true', 'True', 1, '1'] -%}
    {{ return(_via_env or _via_var) }}
{% endmacro %}
