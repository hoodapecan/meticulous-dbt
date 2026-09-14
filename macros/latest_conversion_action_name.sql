{#
    Returns one row per conversion action with its most recent name.

    Fivetran's Google Ads metrics tables key rows on the conversion action
    *name*, not the action id, and carry no soft-delete column. Renaming a
    conversion action in Google Ads therefore does not rewrite history — it
    starts a brand-new row series under the new name, with a clean cut at
    the rename date. The same conversion action ends up with two names.

    Downstream that is fatal, because the vertical models derive
    `metric_name` from the conversion action name:

        lower(regexp_replace(conversion_action_name, '[ -]+', '_'))
            || '_all_conversions'

    One action becomes two metrics, and METICULOUS_MODEL_COLUMNS — which
    maps on the literal metric_name string — can only match one of them.
    Whichever half the operator maps, the other half silently vanishes
    from the wide mart. (MET-157, found on dtca: a goal rename orphaned
    ~$273k of conversion value.)

    This macro picks one canonical name per action — the latest one
    observed (highest date_column) — so every historical row reports
    under the action's current name and a rename self-heals on the next
    dbt run. It is the conversion-action sibling of
    `latest_campaign_name`.

    The action id is Google's resource name
    (`customers/{customer_id}/conversionActions/{id}`), which embeds the
    account, so it stays unique when a client's staging model unions
    several Google Ads accounts.

    Args:
        stg_metrics_ref:   ref() to the staging metrics model. Must expose
                           the action id, name, and date columns below.
        action_id_column:  action resource-name column. Defaults to
                           'conversion_action'.
        name_column:       action name column. Defaults to
                           'conversion_action_name'.
        date_column:       column to order by when picking "latest".
                           Defaults to 'report_date' — pass the client's
                           convention when it differs.

    Usage:
        conversion_action_names as (
            {{ meticulous_dbt.latest_conversion_action_name(ref('stg_google_ads__campaign_metrics')) }}
        )

    Then join by the action id and select the canonical name:

        from {{ ref('stg_google_ads__campaign_metrics') }} m
        left join conversion_action_names a
            on m.conversion_action = a.conversion_action

    Caveat: if a client's raw table ever carries BOTH names on the same
    (action, date) — which a Fivetran Historical Resync can produce —
    canonicalizing merges them and double-counts. Verify before adopting:

        select conversion_action, date, count(distinct conversion_action_name)
        from <raw>.campaign_metrics
        group by 1, 2 having count(distinct conversion_action_name) > 1;
#}
{% macro latest_conversion_action_name(stg_metrics_ref, action_id_column='conversion_action', name_column='conversion_action_name', date_column='report_date') %}
    select
        {{ action_id_column }} as conversion_action,
        {{ name_column }} as conversion_action_name
    from {{ stg_metrics_ref }}
    where {{ action_id_column }} is not null
      and {{ name_column }} is not null
    qualify row_number() over (
        partition by {{ action_id_column }}
        order by {{ date_column }} desc, {{ name_column }} desc
    ) = 1
{% endmacro %}
