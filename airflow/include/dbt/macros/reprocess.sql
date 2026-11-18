{#-
  Targeted reprocessing (edgepulse_reprocess DAG). A run is in reprocess mode when the vars
  reprocess_start and reprocess_end are set: UTC, whole hours, start inclusive, end exclusive.
-#}

{% macro is_reprocess() -%}
  {{ return(var('reprocess_start', none) is not none and var('reprocess_end', none) is not none) }}
{%- endmacro %}

{#- Normal runs rewrite a matched row only when a newer copy arrives (stops write amplification,
    RCA-001). In reprocess mode bronze wins: every matched row in the window is rewritten. -#}
{% macro merge_update_condition(normal_condition) -%}
  {{ return('true' if is_reprocess() else normal_condition) }}
{%- endmacro %}

{#- Filter on event time for the reprocess window. The cast makes it work for bronze
    (timestamp with time zone) and silver (timestamp) alike. -#}
{% macro reprocess_window_filter(column='event_ts') -%}
  where cast({{ column }} as timestamp(6)) >= timestamp '{{ var("reprocess_start") }}'
    and cast({{ column }} as timestamp(6)) <  timestamp '{{ var("reprocess_end") }}'
{%- endmacro %}