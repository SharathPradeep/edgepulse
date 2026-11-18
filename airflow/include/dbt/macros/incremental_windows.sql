{% macro incremental_window() %}
  {% if is_incremental() and is_reprocess() %}
  {{ reprocess_window_filter('event_ts') }}
  {% elif is_incremental() %}
  where cast(_ingest_ts as timestamp(6)) > (
      select max(_ingest_ts) - interval '{{ var("lookback_hours") }}' hour from {{ this }}
  )
  {% endif %}
{% endmacro %}