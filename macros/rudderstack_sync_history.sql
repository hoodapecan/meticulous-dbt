{#
    rudderstack_sync_history

    Returns one row per RudderStack Reverse ETL sync_log entry (connection
    × sync run × primary key), with the payload RudderStack sent for that
    row. Built to be the select of a client-side dbt SNAPSHOT so the
    history outlives RudderStack's own retention (MET-163).

    Why a snapshot is needed at all: RudderStack prunes its own sync
    state. In the warehouse schema it manages (usually `_RUDDERSTACK`):

      SYNC_LOG                   per-row result of each sync — operation
                                 (insert / update / delete), status,
                                 error. Kept ~30 days. Logs CHANGES only,
                                 so a row sent once in July has no trace
                                 by September.
      SNAPSHOT_<CONN>_<RUN>      full copy of the source rows at each sync
                                 (the payload sent). ~6 kept, and the
                                 name rotates every run.

    Payload resolution, per sync_log row:
      'same_run'         the snapshot of that exact run still exists —
                         this is what was sent.
      'latest_snapshot'  that run's snapshot has been pruned; falls back
                         to the newest snapshot holding the key. Carries
                         the row's CURRENT values, which may differ from
                         what was sent if the row was later updated. Only
                         happens for runs older than the snapshot window —
                         in practice, the one-time backfill on first build.
      null               no snapshot holds the key (the row was deleted
                         from the source). Deletes always land here.

    Snapshot tables are discovered at compile time from
    information_schema, because their names rotate. Snapshot names are
    upper-cased; SYNC_LOG ids are mixed-case, so joins compare upper().

    Snowflake-only (object_construct, qualify, information_schema.created).

    Args:
        sync_log:            relation for RudderStack's SYNC_LOG — pass a
                             source(). Snapshot tables are read from the
                             same database + schema.
        primary_key_column:  the column set as the primary key on the
                             RudderStack connection. SYNC_LOG.PRIMARY_KEY
                             holds its value. Defaults to
                             'offline_conversion_key'.
        email_columns:       payload keys holding email addresses. Stored
                             as sha256(lower(trim(value))). Default
                             ['EMAIL'].
        phone_columns:       payload keys holding phone numbers. Stored as
                             sha256(digits only). Default
                             ['PHONE', 'MOBILE_PHONE'].

    PII: RudderStack's snapshots hold emails and phone numbers in plain
    text (JP: every row). This table keeps rows forever, so those keys
    are replaced with SHA-256 hashes, normalised the way Microsoft
    enhanced conversions expects. Still joinable to a hashed CRM export;
    never readable. Keys a payload doesn't carry, or carries as null, are
    left absent.

    Usage (client repo, snapshots/rudderstack_sync_history.sql):

        {% snapshot rudderstack_sync_history %}
        {{ config(
            unique_key='sync_log_key',
            strategy='check',
            check_cols=['operation', 'status', 'error_reason'],
        ) }}
        {{ meticulous_dbt.rudderstack_sync_history(source('rudderstack', 'sync_log')) }}
        {% endsnapshot %}

    The snapshot's unique_key is the sync_log grain, so each run × row
    becomes its own snapshot row, and rows RudderStack prunes from
    SYNC_LOG stay open (hard deletes are ignored by default). The table
    only ever grows: the first build backfills everything SYNC_LOG still
    holds, and every later build appends the new runs.

    check_cols deliberately excludes payload: a row first captured as
    'latest_snapshot' must not be rewritten, and a same-run payload never
    changes after its run.
#}
{% macro rudderstack_sync_history(sync_log, primary_key_column='offline_conversion_key', email_columns=['EMAIL'], phone_columns=['PHONE', 'MOBILE_PHONE']) %}
    {%- set snapshots = [] -%}
    {%- if execute and not meticulous_dbt._skip_runtime_lookups() -%}
        {%- set lookup -%}
            select
                table_name,
                to_varchar(created, 'YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM') as created_at
            from {{ sync_log.database }}.information_schema.tables
            where table_schema = '{{ sync_log.schema | upper }}'
              and table_name like 'SNAPSHOT%'
            order by created
        {%- endset -%}
        {%- for row in run_query(lookup).rows -%}
            {#- SNAPSHOT_<CONN>_<RUN>; neither id contains an underscore -#}
            {%- set parts = row[0].split('_') -%}
            {%- if parts | length == 3 -%}
                {%- do snapshots.append({
                    'table': row[0],
                    'connection': parts[1],
                    'run': parts[2],
                    'created_at': row[1],
                }) -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endif %}

with sync_log as (

    select
        connection_id,
        sync_run_id,
        primary_key,
        operation,
        status,
        error_reason,
        sync_started_at,
        sync_finished_at
    from {{ sync_log }}
    qualify row_number() over (
        partition by connection_id, sync_run_id, primary_key
        order by sync_finished_at desc
    ) = 1

),

raw_snapshots as (

{%- if snapshots | length == 0 %}

    select
        null::varchar as connection_key,
        null::varchar as run_key,
        null::varchar as primary_key,
        null::timestamp_tz as snapshot_created_at,
        null::object as raw_payload
    where false

{%- else %}
{%- for s in snapshots %}

    select
        '{{ s.connection }}' as connection_key,
        '{{ s.run }}' as run_key,
        to_varchar({{ primary_key_column }}) as primary_key,
        '{{ s.created_at }}'::timestamp_tz as snapshot_created_at,
        object_delete(object_construct_keep_null(*), 'RUDDER_OPERATION_TYPE') as raw_payload
    from {{ sync_log.database }}.{{ sync_log.schema }}.{{ s.table }}
    {%- if not loop.last %}

    union all
    {%- endif %}
{%- endfor %}

{%- endif %}

),

{#-
    Each step wraps the previous one but reads its value from raw_payload,
    so the expression grows linearly. A null value drops the key
    (object_insert with update=true), so plaintext never survives.
    Jinja `set` inside a for-loop doesn't escape it; namespace() does.
-#}
{%- set p = namespace(expr='raw_payload') -%}
{%- for col in email_columns -%}
    {%- set p.expr = "object_insert(" ~ p.expr ~ ", '" ~ (col | upper) ~ "', sha2(lower(trim(raw_payload:" ~ (col | upper) ~ "::varchar)), 256), true)" -%}
{%- endfor -%}
{%- for col in phone_columns -%}
    {%- set p.expr = "object_insert(" ~ p.expr ~ ", '" ~ (col | upper) ~ "', sha2(regexp_replace(raw_payload:" ~ (col | upper) ~ "::varchar, '[^0-9]', ''), 256), true)" -%}
{%- endfor %}

snapshots as (

    select
        connection_key,
        run_key,
        primary_key,
        snapshot_created_at,
        {{ p.expr }} as payload
    from raw_snapshots

),

latest_per_key as (

    select
        connection_key,
        primary_key,
        snapshot_created_at,
        payload
    from snapshots
    qualify row_number() over (
        partition by connection_key, primary_key
        order by snapshot_created_at desc
    ) = 1

)

select
    md5(l.connection_id || '|' || l.sync_run_id || '|' || l.primary_key) as sync_log_key,
    l.connection_id,
    l.sync_run_id,
    l.primary_key,
    l.operation,
    l.status,
    l.error_reason,
    l.sync_started_at,
    l.sync_finished_at,
    coalesce(same_run.payload, latest.payload) as payload,
    case
        when same_run.payload is not null then 'same_run'
        when latest.payload is not null then 'latest_snapshot'
    end as payload_source,
    coalesce(same_run.snapshot_created_at, latest.snapshot_created_at) as payload_snapshot_created_at
from sync_log as l
left join snapshots as same_run
    on
        same_run.connection_key = upper(l.connection_id)
        and same_run.run_key = upper(l.sync_run_id)
        and same_run.primary_key = l.primary_key
left join latest_per_key as latest
    on
        latest.connection_key = upper(l.connection_id)
        and latest.primary_key = l.primary_key

{% endmacro %}
