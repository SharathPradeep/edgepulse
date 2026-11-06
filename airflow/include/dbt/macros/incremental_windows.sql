{% macro incremental_window() %}
  {% if is_incremental() %}
  where cast(_ingest_ts as timestamp(6)) > (
      select max(_ingest_ts) - interval '{{ var("lookback_hours") }}' hour from {{ this }}
  )
  {% endif %}
{% endmacro %}