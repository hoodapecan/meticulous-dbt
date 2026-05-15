{#
    meticulous_conversion_columns

    Generates the conversion-column section of the wide
    meticulous__campaign_performance mart by reading
    METICULOUS_MODEL_COLUMNS at compile time.

    Each row in METICULOUS_MODEL_COLUMNS describes a contribution from
    one source metric to one wide column. Rows that share the same
    DISPLAY_ALIAS are unioned together into a single output column:

        coalesce(sum(case when metric_name in ('m1', 'm2', ...) then
                          metric_value end), 0) as <alias>

    This N:1 behaviour is what lets a client define a single "purchases"
    column that adds Google's Purchase_all_conversions, Meta's
    omni_purchase_*, and Bing's Purchase_count together. A row with a
    unique alias still emits a single-element IN clause and is
    semantically identical to the original 1:1 implementation.

    The column alias is derived from display_alias (lowercased, special
    characters replaced with underscores). If display_alias is empty,
    falls back to a sanitized version of metric_name. Column order
    follows the lowest POSITION among each alias's member rows.

    Designed to be dropped into the `pivoted` CTE of
    meticulous__campaign_performance — emits a comma-prefixed list so
    it can sit at the end of an existing comma-separated select.

    If METICULOUS_MODEL_COLUMNS does not exist yet (fresh client) or is
    empty, the macro emits nothing — the mart compiles fine with just
    the universal floor (impressions, clicks, spend).

    Usage in meticulous__campaign_performance.sql, inside the pivoted CTE:

        select
            v.report_date,
            v.platform,
            v.campaign_id,
            max(v.campaign_name) as campaign_name,
            coalesce(sum(case when v.metric_name = 'impressions' then v.metric_value end), 0) as impressions,
            coalesce(sum(case when v.metric_name = 'clicks' then v.metric_value end), 0) as clicks,
            coalesce(sum(case when v.metric_name = 'spend' then v.metric_value end), 0) as spend
            {{ meticulous_dbt.meticulous_conversion_columns(source('meticulous', 'meticulous_model_columns')) }}
        from vertical as v
        group by v.report_date, v.platform, v.campaign_id

    The outer `select` should then reference `p.*` (or list known
    columns + spread the dynamic ones).
#}

{% macro meticulous_conversion_columns(model_columns_source, vertical_alias='v') %}

{#- Discover rows at compile time. -#}
{#- We tolerate missing table (fresh client) by guarding with execute and -#}
{#- defaulting to []. -#}
{%- set rows = [] -%}
{%- if execute -%}
    {%- set query -%}
        select metric_name, display_alias
        from {{ model_columns_source }}
        order by position, metric_name
    {%- endset -%}

    {%- set results = run_query(query) -%}
    {%- if results and results.rows is not none and results.rows | length > 0 -%}
        {%- set metric_col = results.columns[0].values() -%}
        {%- set alias_col = results.columns[1].values() -%}
        {%- for i in range(metric_col | length) -%}
            {%- set _ = rows.append({'metric_name': metric_col[i], 'display_alias': alias_col[i]}) -%}
        {%- endfor -%}
    {%- endif -%}
{%- endif -%}

{#- Group rows by sanitized display_alias. Insertion order = position order, -#}
{#- so the first row to introduce an alias sets its column position. -#}
{%- set group_order = [] -%}
{%- set groups = {} -%}
{%- for row in rows -%}
    {%- set raw = row.display_alias if row.display_alias else row.metric_name -%}
    {%- set safe = raw | lower -%}
    {%- set safe = safe | replace(' ', '_') -%}
    {%- set safe = safe | replace('-', '_') -%}
    {%- set safe = safe | replace('.', '_') -%}
    {%- set safe = safe | replace('/', '_') -%}
    {%- set safe = safe | replace(',', '_') -%}
    {%- set safe = safe | replace('(', '') -%}
    {%- set safe = safe | replace(')', '') -%}
    {%- set safe = safe | replace('[', '') -%}
    {%- set safe = safe | replace(']', '') -%}
    {%- set safe = safe | replace('{', '') -%}
    {%- set safe = safe | replace('}', '') -%}
    {%- set safe = safe | replace("'", '') -%}
    {%- set safe = safe | replace('"', '') -%}
    {%- set safe = safe | replace('&', 'and') -%}
    {%- if safe not in groups -%}
        {%- set _ = group_order.append(safe) -%}
        {%- set _ = groups.update({safe: []}) -%}
    {%- endif -%}
    {#- Skip exact metric_name duplicates inside the same alias (no effect on output, keeps IN-clause clean). -#}
    {%- if row.metric_name not in groups[safe] -%}
        {%- set _ = groups[safe].append(row.metric_name) -%}
    {%- endif -%}
{%- endfor -%}

{%- for alias in group_order -%}
    ,
    coalesce(sum(case when {{ vertical_alias }}.metric_name in (
        {%- for m in groups[alias] -%}
            '{{ m | replace("'", "''") }}'{% if not loop.last %}, {% endif %}
        {%- endfor -%}
    ) then {{ vertical_alias }}.metric_value end), 0) as {{ alias }}
{%- endfor -%}

{% endmacro %}
