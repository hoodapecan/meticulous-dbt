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

## What stays client-specific

- **Conversion metrics** — the CASE WHEN pivots for platform-specific conversions (purchases, leads, etc.)
- **Source definitions** — database/schema names vary per client
- **Platform staging models** — each client has different ad platforms and table structures

## What the package handles

- Dynamic taxonomy dimension discovery
- Taxonomy pivot, select, join, and group-by generation
- Reserved keyword quoting (e.g., EVENT)
