{#
    Returns one row per campaign_id with the most recent campaign_name.

    Some clients keep one staging-dim row per (campaign_id, date) — so a
    renamed campaign produces multiple rows: the old name on early dates,
    the new name on later dates. Downstream models that join the dim by
    (campaign_id, date) then carry the *historical* name on every metric
    row, and the same campaign ends up with two names in the wide mart.
    The Meticulous app then sees the campaign as two separate entities,
    breaks the campaigns_cache upsert (ON CONFLICT collision), and the
    campaign goes missing from Budgets / Pacing.

    This macro picks one canonical name per campaign — the latest one
    observed (highest date_column) — so downstream joins on campaign_id
    alone get a single, current name.

    Args:
        stg_dim_ref: ref() to the staging dim. Must expose campaign_id,
                     campaign_name, and the column named in date_column.
        date_column: column to order by when picking "latest". Defaults
                     to 'report_date' — pass 'calendardate' (or whatever
                     the client convention is) when needed.

    Usage:
        campaign_names as (
            {{ meticulous_dbt.latest_campaign_name(ref('stg_google_ads__campaign_dim')) }}
        )

    Then join downstream by campaign_id alone (drop the date join key).
#}
{% macro latest_campaign_name(stg_dim_ref, date_column='report_date') %}
    select campaign_id, campaign_name
    from {{ stg_dim_ref }}
    qualify row_number() over (
        partition by campaign_id
        order by {{ date_column }} desc
    ) = 1
{% endmacro %}
