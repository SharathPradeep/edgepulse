{% docs event_id %}Unique event identifier (UUID) set by the device. Primary key in silver.{% enddocs %}

{% docs device_id %}Gateway that produced the event. Joins to dim_device.{% enddocs %}

{% docs event_ts %}When the event happened on the device (event time, UTC).{% enddocs %}

{% docs ingest_ts %}When the consumer wrote the event to bronze (arrival time, UTC). Drives incremental loads.{% enddocs %}

{% docs is_late %}True when the event arrived more than 15 minutes after it happened.{% enddocs %}

{% docs kafka_partition %}Kafka partition of the source message, for lineage and replay.{% enddocs %}

{% docs kafka_offset %}Kafka offset of the source message, for lineage and replay.{% enddocs %}

{% docs device_hour_key %}Grain key: device_id and event_hour joined with a pipe.{% enddocs %}

{% docs event_hour %}Event time truncated to the hour (UTC).{% enddocs %}

{% docs source_max_ingest_ts %}Latest silver arrival time included in this row. Watermark for late-data recompute.{% enddocs %}