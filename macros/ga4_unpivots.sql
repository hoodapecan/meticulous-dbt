{#
    GA4 into the campaign_performance vertical (JP-27, agency GA4 standard;
    first built for MLV in MET-165).

    Each client exposes three staging models with these columns, whatever
    connector loaded the data (Fivetran, Airbyte, ...). Connector quirks
    (column case, dedupe, date parsing) stay inside those staging models.

      stg_ga4__traffic_acquisition   (session source/medium report)
        report_date date, session_source, session_medium, sessions,
        engaged_sessions, total_users, user_engagement_duration,
        key_events (null if the report doesn't carry it), total_revenue

      stg_ga4__user_acquisition      (first-user source/medium report)
        report_date date, first_user_source, first_user_medium,
        new_users, returning_users

      stg_ga4__events                (events report)
        report_date date, event_name, event_count, total_revenue

    Client models are one-liners:

      -- int_ga4__channel_metrics_unpivot.sql
      {{ meticulous_dbt.ga4_channel_metrics_unpivot(
          ref('stg_ga4__traffic_acquisition'), ref('stg_ga4__user_acquisition')) }}

      -- int_ga4__events_unpivot.sql
      {{ meticulous_dbt.ga4_events_unpivot(ref('stg_ga4__events')) }}

    Output columns for both: platform ('GA4'), data_source ('ga4'),
    campaign_id, campaign_name, report_date, metric_name, metric_value.
#}


{#
    GA4 site traffic by channel group. One sentinel "campaign" per channel
    group per day:
      paid_search, paid_social, display, organic_search, organic_social,
      direct, referral, email, unassigned

    Channel rules: ga4_channel_rules_sql() (package defaults plus the
    client's meticulous_ga4_extra_channel_rules var). Unmatched pairs go to
    'unassigned', GA4's own name for them; deliberately not 'unattributed',
    which the events row uses.

    metric_name (prefixed site_ so they never blend into paid totals):
      traffic report:  site_sessions, site_engaged_sessions, site_users,
                       site_engagement_seconds, site_key_events, site_revenue
      user report:     site_new_users, site_returning_users

    site_users / site_returning_users are daily GA4 counts and are not unique
    across days or channels; site_sessions is the additive measure. Returning
    users are bucketed by the channel that first acquired them.
#}
{% macro ga4_channel_metrics_unpivot(traffic_relation, users_relation) %}

with rules as (

    {{ meticulous_dbt.ga4_channel_rules_sql() }}

),

traffic as (

    select
        report_date,
        lower(coalesce(session_source, '(not set)')) as source,
        lower(coalesce(session_medium, '(not set)')) as medium,
        sessions,
        engaged_sessions,
        total_users,
        user_engagement_duration,
        key_events,
        total_revenue
    from {{ traffic_relation }}

),

user_acquisition as (

    select
        report_date,
        lower(coalesce(first_user_source, '(not set)')) as source,
        lower(coalesce(first_user_medium, '(not set)')) as medium,
        new_users,
        returning_users
    from {{ users_relation }}

),

source_mediums as (

    select source, medium from traffic
    union
    select source, medium from user_acquisition

),

channel_map as (

    select
        sm.source,
        sm.medium,
        coalesce(r.channel_group, 'unassigned') as channel_group
    from source_mediums as sm
    left join rules as r
        on regexp_like(sm.source, '^(' || r.source_regex || ')$')
       and regexp_like(sm.medium, '^(' || r.medium_regex || ')$')
    qualify row_number() over (
        partition by sm.source, sm.medium
        order by r.priority nulls last
    ) = 1

),

traffic_daily as (

    select
        t.report_date,
        m.channel_group,
        sum(t.sessions)::float                 as site_sessions,
        sum(t.engaged_sessions)::float         as site_engaged_sessions,
        sum(t.total_users)::float              as site_users,
        sum(t.user_engagement_duration)::float as site_engagement_seconds,
        sum(t.key_events)::float               as site_key_events,
        sum(t.total_revenue)::float            as site_revenue
    from traffic as t
    inner join channel_map as m
        on t.source = m.source
       and t.medium = m.medium
    group by 1, 2

),

users_daily as (

    select
        u.report_date,
        m.channel_group,
        sum(u.new_users)::float       as site_new_users,
        sum(u.returning_users)::float as site_returning_users
    from user_acquisition as u
    inner join channel_map as m
        on u.source = m.source
       and u.medium = m.medium
    group by 1, 2

),

traffic_unpivoted as (

    select report_date, channel_group, metric_name, metric_value
    from traffic_daily
    unpivot (
        metric_value for metric_name in (
            site_sessions,
            site_engaged_sessions,
            site_users,
            site_engagement_seconds,
            site_key_events,
            site_revenue
        )
    )

),

users_unpivoted as (

    select report_date, channel_group, metric_name, metric_value
    from users_daily
    unpivot (
        metric_value for metric_name in (
            site_new_users,
            site_returning_users
        )
    )

),

unioned as (

    select * from traffic_unpivoted
    union all
    select * from users_unpivoted

)

select
    'GA4' as platform,
    'ga4' as data_source,
    channel_group as campaign_id,
    case channel_group
        when 'paid_search'    then 'Paid Search'
        when 'paid_social'    then 'Paid Social'
        when 'display'        then 'Display'
        when 'organic_search' then 'Organic Search'
        when 'organic_social' then 'Organic Social'
        when 'direct'         then 'Direct'
        when 'referral'       then 'Referral'
        when 'email'          then 'Email'
        else 'Unassigned'
    end as campaign_name,
    report_date,
    lower(metric_name) as metric_name,
    metric_value
from unioned
where metric_value is not null
  and metric_value <> 0

{% endmacro %}


{#
    GA4 events by name, site-grain (the events report has no source/medium).
    campaign_id / campaign_name are NULL here; the client's cross-platform
    union puts them on a per-day 'unattributed' row.

    metric_name: {event_name} = event_count; {event_name}_revenue = revenue
    when non-zero. Names are lowercased BEFORE grouping: GA4 event names are
    case-sensitive, so 'Impression' and 'impression' must sum into one metric.
#}
{% macro ga4_events_unpivot(events_relation) %}

with events as (

    select * from {{ events_relation }}

),

event_counts as (

    select
        report_date,
        lower(event_name) as metric_name,
        sum(event_count)::float as metric_value
    from events
    where event_count is not null
      and event_count <> 0
    group by 1, 2

),

event_revenue as (

    select
        report_date,
        lower(event_name) || '_revenue' as metric_name,
        sum(total_revenue)::float as metric_value
    from events
    where total_revenue is not null
      and total_revenue <> 0
    group by 1, 2

),

union_all as (

    select * from event_counts
    union all
    select * from event_revenue

)

select
    'GA4' as platform,
    'ga4' as data_source,
    cast(null as varchar) as campaign_id,
    cast(null as varchar) as campaign_name,
    report_date,
    lower(metric_name) as metric_name,
    metric_value
from union_all

{% endmacro %}


{#
    Singular-test body: returns any date where the channel rows' site_sessions
    differ from the traffic report's total. Channel groups must add up to
    total sessions every day.

      -- tests/assert_ga4_channel_sessions_reconcile.sql
      {{ meticulous_dbt.ga4_channel_sessions_reconcile(
          ref('stg_ga4__traffic_acquisition'), ref('int_ga4__channel_metrics_unpivot')) }}
#}
{% macro ga4_channel_sessions_reconcile(traffic_relation, channels_relation) %}

with report as (

    select report_date, sum(sessions) as sessions
    from {{ traffic_relation }}
    group by 1

),

channels as (

    select report_date, sum(metric_value) as sessions
    from {{ channels_relation }}
    where metric_name = 'site_sessions'
    group by 1

)

select
    r.report_date,
    r.sessions as report_sessions,
    coalesce(c.sessions, 0) as channel_sessions
from report as r
left join channels as c
    on r.report_date = c.report_date
where coalesce(r.sessions, 0) <> coalesce(c.sessions, 0)

{% endmacro %}
