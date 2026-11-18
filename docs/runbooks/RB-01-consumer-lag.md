# RB-01: Consumer lag

| | |
|---|---|
| Symptom | Lag for group `edgepulse-bronze-loader` keeps growing in Kafka UI. Often followed by a freshness warning (RB-03) |
| Alert | None directly. Lag is not monitored from Airflow, so the bronze freshness check is the indirect alarm |
| Data at risk | None if fixed within Kafka's retention (7 days, the default). Offsets are committed only after a successful write |

## What it means

Lag is the number of messages Kafka holds that the consumer has not yet written to bronze. A small lag that rises and falls each minute is normal, because the consumer writes in batches of up to 5,000 records or 60 seconds. Lag that only grows means the consumer is stopped, failing or slower than the producers.

## Check

1. **Kafka UI > Consumers > `edgepulse-bronze-loader`.** Note the total lag and the state. `Empty` means no consumer is connected.
2. **Is the container running?**
   ```bash
   docker compose ps consumer
   ```
3. **What is it doing?**
   ```bash
   docker compose logs --tail 100 consumer
   ```
   A healthy consumer prints `Flushing batch` and `wrote N records` about once a minute.

## Fix

| What you see | Cause | Fix |
|---|---|---|
| Container not running, group `Empty` | Consumer stopped | `docker compose --profile pipeline up -d consumer` |
| Container `Restarting`, a traceback in the logs | Crash loop | Read the last error. `AccessDenied` or expired credentials: check the `edgepulse` AWS profile. `records carry fields missing from the latest contract`: go to RB-04 |
| Repeated `Attempt N failed ... Retrying` | S3, Glue or network errors | Backoff retries 1, 2, 4 and 8 seconds. If it persists, fix the error shown, then restart the consumer |
| Logs healthy, lag still growing | Producers sending faster than one consumer can write | Wait for it to catch up. In production, add consumers (up to one per partition) |

## Verify

- Lag drops back to under one batch per topic and stays there.
- `wrote N records` lines appear every minute.
- The next `edgepulse_hourly` run passes the bronze freshness tasks.

## After a long outage

No reprocessing is needed. Silver reads bronze by arrival time with a 2-hour lookback, so the backlog flows into silver and gold on the next hourly run. Expect `silver_rows_last_hour` to error above 200,000 rows during a large catch-up. That is the backlog arriving, not a fault.

## In production

Alert on lag from MSK or CloudWatch metrics and scale consumers on ECS or EKS based on lag.