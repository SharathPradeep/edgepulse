-- Every bronze event up to silver's watermark must exist in silver
select b.event_id
from {{ source('bronze', 'request_log') }} b
left join {{ ref('stg_request_log') }} s on b.event_id = s.event_id
where s.event_id is null
  and cast(b._ingest_ts as timestamp(6)) <= (select max(_ingest_ts) from {{ ref('stg_request_log') }})