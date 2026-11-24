{#
    GA4 channel rules (JP-27, agency GA4 standard; first built for MLV in MET-165).

    Maps a lowercased GA4 source/medium pair to a Meticulous channel group:
      paid_search, paid_social, display, organic_search, organic_social,
      direct, referral, email   (anything unmatched -> 'unassigned')

    Rules are tried in priority order; the first rule whose source_regex AND
    medium_regex both FULLY match wins (the regexes are anchored with ^( )$).
    The paid rules must keep matching the utm_medium values the Meticulous
    taxonomy writes (facebook/cpc, google/cpc, bing/cpc).

    The defaults avoid backslashes ([(] instead of \( ) so they survive SQL
    string literals unchanged. Client rules are escaped before they are
    written into SQL, so they may use either form.

    Client-specific rules go in the client's dbt_project.yml and are merged
    with the defaults (lower priority number = tried first):

        vars:
          meticulous_ga4_extra_channel_rules:
            - {priority: 45, channel_group: display,
               source_regex: 'nbc4|wcmh', medium_regex: '.*',
               note: 'Local media partners: paid even when tagged social'}
#}

{% macro ga4_default_channel_rules() %}
    {{ return([
        {'priority': 10, 'channel_group': 'direct',
         'source_regex': '[(]direct[)]',
         'medium_regex': '[(]none[)]|[(]not set[)]',
         'note': 'GA4 direct'},

        {'priority': 20, 'channel_group': 'paid_social',
         'source_regex': 'fb|fb-sitelink.*|facebook|ig|instagram|an|msg|meta|th|threads|tiktok|x|twitter|linkedin|reddit|snapchat|pinterest|aod|geofence',
         'medium_regex': 'cpc|ppc|paid|paid[ _]social|static|video|feed|meta|stories|story|reels|carousel',
         'note': 'Social networks with a paid medium: taxonomy UTMs (facebook/cpc), Meta dynamic params, Meta format mediums'},

        {'priority': 25, 'channel_group': 'paid_social',
         'source_regex': '.*',
         'medium_regex': 'paid[ _]social|paid social',
         'note': 'Explicit paid-social medium from any source'},

        {'priority': 30, 'channel_group': 'paid_search',
         'source_regex': '.*',
         'medium_regex': 'cpc|ppc|paid[ _]search|paid search|sem',
         'note': 'Taxonomy UTMs (google/cpc, bing/cpc) and other paid-search mediums'},

        {'priority': 40, 'channel_group': 'display',
         'source_regex': 'stackadapt|mobilefuse|mf|mf_.*|nexstar|localiq|tradedesk|thetradedesk|siteimpact|eblast|display',
         'medium_regex': '.*',
         'note': 'Programmatic and paid-placement vendors regardless of medium'},

        {'priority': 41, 'channel_group': 'display',
         'source_regex': '.*',
         'medium_regex': 'display|banner|video|ott|ctv|streamingtv|addressable ctv.*|premium ott|livestream.*|pre[ -]?roll|audio|programmatic|native|cpm',
         'note': 'Display / video / CTV / audio mediums'},

        {'priority': 42, 'channel_group': 'display',
         'source_regex': 'google',
         'medium_regex': 'dg|demand[ _]?gen',
         'note': 'Google Demand Gen'},

        {'priority': 50, 'channel_group': 'email',
         'source_regex': '.*',
         'medium_regex': 'email|e-mail|newsletter|eblast|tmemail|et-email',
         'note': 'Owned email and newsletters'},

        {'priority': 51, 'channel_group': 'email',
         'source_regex': 'klaviyo|hs_email|sfmc|mailchimp',
         'medium_regex': '.*',
         'note': 'Email platforms regardless of medium'},

        {'priority': 60, 'channel_group': 'organic_search',
         'source_regex': '.*',
         'medium_regex': 'organic',
         'note': 'GA4 organic search'},

        {'priority': 61, 'channel_group': 'organic_search',
         'source_regex': 'google|bing|yahoo|duckduckgo|ecosia|naver|baidu|yandex|aol|ask',
         'medium_regex': 'search',
         'note': 'Search engine with a non-standard medium'},

        {'priority': 70, 'channel_group': 'organic_social',
         'source_regex': '.*',
         'medium_regex': 'social|social[-_ ]network|social[-_ ]media|sm|post|bio|link[-_ ]in[-_ ]bio',
         'note': 'Organic social mediums'},

        {'priority': 71, 'channel_group': 'organic_social',
         'source_regex': '(.*[.])?(facebook[.]com|instagram[.]com|t[.]co|x[.]com|twitter[.]com|tiktok[.]com|youtube[.]com|linkedin[.]com|reddit[.]com|linktr[.]ee|threads[.]net|pinterest[.]com)',
         'medium_regex': 'referral',
         'note': 'Untagged clicks from social networks'},

        {'priority': 80, 'channel_group': 'referral',
         'source_regex': '.*',
         'medium_regex': 'referral|ai-assistant|qr[ _]code|bag tag|app|website|affiliate',
         'note': 'Referrals and offline-to-online links'},

        {'priority': 81, 'channel_group': 'referral',
         'source_regex': '[^(].*',
         'medium_regex': '[(]not set[)]',
         'note': 'Named source with no medium (e.g. chatgpt.com)'}
    ]) }}
{% endmacro %}


{# A SQL string literal that Snowflake reads back as exactly `value`. #}
{% macro _ga4_sql_string(value) -%}
    '{{ value | string | replace("\\", "\\\\") | replace("'", "''") }}'
{%- endmacro %}


{#
    Renders the merged rule set as a SELECT (priority, channel_group,
    source_regex, medium_regex). Use as a CTE body.
#}
{% macro ga4_channel_rules_sql() %}
    {%- set rules = meticulous_dbt.ga4_default_channel_rules() + var('meticulous_ga4_extra_channel_rules', []) -%}
    select
        column1::integer as priority,
        column2::varchar as channel_group,
        column3::varchar as source_regex,
        column4::varchar as medium_regex
    from values
    {%- for r in rules %}
        ({{ r['priority'] | int }}, {{ meticulous_dbt._ga4_sql_string(r['channel_group']) }}, {{ meticulous_dbt._ga4_sql_string(r['source_regex']) }}, {{ meticulous_dbt._ga4_sql_string(r['medium_regex']) }}){{ "," if not loop.last }}
    {%- endfor %}
{% endmacro %}
