# EdgePulse runbooks

Step-by-step responses for the failures this pipeline is built to survive. Start from the symptom.

| Symptom | Runbook |
|---|---|
| Consumer lag growing in Kafka UI, or bronze arriving late | [RB-01 Consumer lag](RB-01-consumer-lag.md) |
| `gateway.dlq` message count rising, or consumer logs "sent to DLQ" | [RB-02 DLQ spike](RB-02-dlq-spike.md) |
| Hourly DAG fails on a bronze freshness task | [RB-03 Freshness breach](RB-03-freshness-breach.md) |
| `register_schemas.py` prints INCOMPATIBLE, a producer cannot find its schema or the consumer reports unknown fields | [RB-04 Incompatible schema](RB-04-incompatible-schema.md) |
| `run_dq_checks` fails on `bronze_duplicate_rate` | [RB-05 Duplicate surge](RB-05-duplicate-surge.md) |
| Silver or gold is wrong for a known time window | [RB-06 Data repair](RB-06-data-repair.md) |

## Ground rules

1. **No manual INSERT, UPDATE or DELETE on silver or gold.** Bronze is the source of truth. Repairs go through `edgepulse_reprocess` ([RB-06](RB-06-data-repair.md)). See [RCA-002](../rca/RCA-002-silver-damaged-during-duplicate-incident.md) for why.
2. **Never delete from bronze.** It is the raw record and the only way to rebuild silver and gold.
3. **Never lower a schema subject's compatibility to force a change through.**
4. **Stop the generator and consumer when not working.** They create files and Iceberg snapshots every minute.

## Where to look

| Tool | Address | Used for |
|---|---|---|
| Kafka UI | http://localhost:8085 | Topics, consumer group `edgepulse-bronze-loader` lag, DLQ messages, Schema Registry subjects |
| Airflow | http://localhost:8080 | `edgepulse_hourly`, `edgepulse_reprocess`, `edgepulse_maintenance` runs and logs (alerts log as `ALERT`) |
| Athena | Workgroup `edgepulse`, eu-north-1 | Bronze, silver and gold queries, `ep_ops.dq_results`, Iceberg `$snapshots` |
| Consumer logs | `docker compose logs --tail 100 consumer` | Batch flushes, DLQ counts, retries, schema evolution |

Recent health check results:
```sql
select checked_at, check_name, metric_value, status
from ep_ops.dq_results
where checked_at > cast(current_timestamp at time zone 'UTC' as timestamp(6)) - interval '1' day
order by checked_at desc, check_name;
```

All times are UTC.