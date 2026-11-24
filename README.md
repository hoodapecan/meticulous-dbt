# meticulous-dbt

Shared dbt macros for the Meticulous platform. Provides dynamic taxonomy dimension handling so client dbt projects don't hardcode taxonomy field names.

## Installation

Add to your client project's `packages.yml`:

```yaml
packages:
  - git: "https://github.com/hoodapecan/meticulous-dbt.git"
    revision: main
```

Then run `dbt deps`.

## Prerequisites

Your project must have a source defined for the Meticulous taxonomy mappings table:

```yaml
# models/staging/meticulous/meticulous_sources.yml
sources:
  - name: meticulous
    database: "{{ your_database }}"
    schema: "{{ your_schema }}"
    tables:
      - name: meticulous_taxonomy_mappings
```

## Macros

### `meticulous_taxonomy_pivot`

Generates a complete staging model that pivots `METICULOUS_TAXONOMY_MAPPINGS` from vertical to wide format. Discovers field names dynamically at compile time.

```sql
-- models/staging/meticulous/stg_meticulous__taxonomy_mappings.sql
{{ meticulous_dbt.meticulous_taxonomy_pivot(
    source('meticulous', 'meticulous_taxonomy_mappings')
) }}
```

### `meticulous_taxonomy_columns`

Generates a comma-separated list of taxonomy columns for SELECT statements.

```sql
select
    u.platform,
    u.campaign_id,
    u.campaign_name,
    u.report_date,
    u.metric_name,
    u.metric_value,
    {{ meticulous_dbt.meticulous_taxonomy_columns(
        source('meticulous', 'meticulous_taxonomy_mappings'),
        alias='t'
    ) }}
from unioned u
left join {{ ref('stg_meticulous__taxonomy_mappings') }} t
    on lower(u.platform) = lower(t.platform)
    and u.campaign_id = t.campaign_id
```

### `meticulous_taxonomy_group_by`

Generates numbered group-by references for taxonomy columns.

```sql
group by 1, 2, 3, 4  -- non-taxonomy columns
    {{ meticulous_dbt.meticulous_taxonomy_group_by(
        source('meticulous', 'meticulous_taxonomy_mappings'),
        offset=4
    ) }}
```

### `meticulous_taxonomy_join_conditions`

Generates JOIN conditions for budget pacing (null-safe matching).

```sql
left join actual_spend s
    on bd.report_date = s.report_date
    {{ meticulous_dbt.meticulous_taxonomy_join_conditions(
        source('meticulous', 'meticulous_taxonomy_mappings'),
        left_alias='bd',
        right_alias='s'
    ) }}
```

### `latest_campaign_name`

Returns one canonical name per `campaign_id` — the latest observed. Renamed campaigns otherwise carry their historical name on every metric row and split into two entities downstream.

```sql
campaign_names as (
    {{ meticulous_dbt.latest_campaign_name(ref('stg_google_ads__campaign_dim')) }}
)
```

### `latest_conversion_action_name`

The conversion-action sibling of `latest_campaign_name`. Fivetran keys Google Ads metrics rows on the conversion action *name*, so renaming a goal starts a new row series instead of rewriting history — one action becomes two `metric_name` values, and `METICULOUS_MODEL_COLUMNS` can only match one of them. This picks the latest name per action resource id and applies it across all history, so a rename self-heals on the next run.

```sql
conversion_action_names as (
    {{ meticulous_dbt.latest_conversion_action_name(ref('stg_google_ads__campaign_metrics')) }}
),

conv_agg as (
    select
        m.campaign_id,
        m.report_date,
        lower(regexp_replace(a.conversion_action_name, '[ -]+', '_')) || '_all_conversions' as metric_name,
        sum(m.all_conversions)::float as metric_value
    from {{ ref('stg_google_ads__campaign_metrics') }} m
    left join conversion_action_names a
        on m.conversion_action = a.conversion_action
    group by 1, 2, 3
)
```

## What stays client-specific

- **Conversion metrics** — the CASE WHEN pivots for platform-specific conversions (purchases, leads, etc.)
- **Source definitions** — database/schema names vary per client
- **Platform staging models** — each client has different ad platforms and table structures

## What the package handles

- Dynamic taxonomy dimension discovery
- Taxonomy pivot, select, join, and group-by generation
- Reserved keyword quoting (e.g., EVENT)

### `rudderstack_sync_history`

Select body for a client **snapshot** that persists RudderStack Reverse ETL sync state past RudderStack's own retention (`SYNC_LOG` ~30 days, per-run `SNAPSHOT_<conn>_<run>` tables ~3 days). One row per connection × sync run × primary key, with the payload that was sent. Emails and phones are stored as SHA-256 hashes. Snowflake-only. See the macro header for payload resolution (`same_run` / `latest_snapshot` / null) and snapshot config (MET-163).

```sql
-- snapshots/rudderstack_sync_history.sql
{% snapshot rudderstack_sync_history %}
{{ config(unique_key='sync_log_key', strategy='check', check_cols=['operation', 'status', 'error_reason']) }}
{{ meticulous_dbt.rudderstack_sync_history(source('rudderstack', 'sync_log'), primary_key_column='offline_conversion_key') }}
{% endsnapshot %}
```


## GA4 standard (JP-27)

GA4 site traffic lands in `campaign_performance` the same way for every client, whichever connector (Fivetran or Airbyte) loads it.

**Reports to sync** (GA4 Data API, daily): traffic acquisition by session source/medium, traffic acquisition by session campaign, user acquisition by first-user source/medium, events. Include `keyEvents` in the acquisition reports where the connector allows it.

**Client staging contract.** Each client writes three thin staging models. Connector quirks (column case, duplicate rows, `YYYYMMDD` dates) are handled there and nowhere else.

| Model | Columns |
|---|---|
| `stg_ga4__traffic_acquisition` | `report_date`, `session_source`, `session_medium`, `sessions`, `engaged_sessions`, `total_users`, `user_engagement_duration`, `key_events` (null if not synced), `total_revenue` |
| `stg_ga4__user_acquisition` | `report_date`, `first_user_source`, `first_user_medium`, `new_users`, `returning_users` |
| `stg_ga4__events` | `report_date`, `event_name`, `event_count`, `total_revenue` |

**Shared logic** (one-line client models):

```sql
-- models/intermediate/int_ga4__channel_metrics_unpivot.sql
{{ meticulous_dbt.ga4_channel_metrics_unpivot(
    ref('stg_ga4__traffic_acquisition'), ref('stg_ga4__user_acquisition')) }}

-- models/intermediate/int_ga4__events_unpivot.sql
{{ meticulous_dbt.ga4_events_unpivot(ref('stg_ga4__events')) }}

-- tests/assert_ga4_channel_sessions_reconcile.sql
{{ meticulous_dbt.ga4_channel_sessions_reconcile(
    ref('stg_ga4__traffic_acquisition'), ref('int_ga4__channel_metrics_unpivot')) }}
```

- Channel rows: one sentinel campaign per channel group per day (`paid_search`, `paid_social`, `display`, `organic_search`, `organic_social`, `direct`, `referral`, `email`, `unassigned`) carrying `site_*` metrics. Platform `GA4`, data_source `ga4`.
- Event rows: one metric per event name (plus `<event>_revenue`). The client's cross-platform union puts them on campaign_id `unattributed`.

**Channel rules** live in `ga4_default_channel_rules()`. Add client rules in `dbt_project.yml`; they merge with the defaults, lowest priority number first:

```yaml
vars:
  meticulous_ga4_extra_channel_rules:
    - {priority: 45, channel_group: display, source_regex: 'nbc4|wcmh', medium_regex: '.*'}
```

Regexes are matched against the lowercased source/medium and must match the whole value.
