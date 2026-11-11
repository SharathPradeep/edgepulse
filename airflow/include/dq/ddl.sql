CREATE TABLE ep_ops.dq_results (
  run_id string,
  check_name string,
  table_name string,
  metric_value double,
  status string,
  thresholds string,
  checked_at timestamp
)
PARTITIONED BY (day(checked_at))
LOCATION 's3://edgepulse-lake-sharath-0001/ep_ops/dq_results/'
TBLPROPERTIES ('table_type' = 'ICEBERG');