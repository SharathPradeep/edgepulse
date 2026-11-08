{% set models = ['stg_request_log', 'stg_waf_alert', 'stg_device_metric'] %}

{% for m in models %}
select '{{ m }}' as model_name, event_id, event_ts
from {{ ref(m) }}
where event_ts > cast(current_timestamp at time zone 'UTC' as timestamp(6)) + interval '5' minute
{% if not loop.last %}union all{% endif %}
{% endfor %}