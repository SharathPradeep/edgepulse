# EdgePulse RAID log

Risks, assumptions, issues and dependencies for the project as built. Status as of 18 November 2026.

## Risks

| ID | Risk | Impact | Mitigation |
|---|---|---|---|
| R1 | The AWS Free plan ends after six months or when credits run out, and the account then closes | Lake, catalogue and query history lost after the 90-day retention period | Stop services when idle and watch the credit balance. Keep code, docs and evidence in Git so the platform can be rebuilt |
| R2 | Kafka runs without a volume, so `docker compose down` deletes topics, offsets and registered schemas | Local streaming state lost (bronze on S3 is unaffected) | Use `stop`, not `down`. `register_schemas.py` re-registers every contract version in order |
| R3 | Long-lived IAM access keys on the laptop (`~/.aws`, `airflow/.env`) | Key leak would expose the bucket, catalogue and workgroup | Least-privilege policy (`infra/aws/ep-dev-policy.json`), push protection on GitHub, keys deleted at close-down. Production: IAM roles and OIDC |
| R4 | The PII salt appears in plain text in compiled SQL and Athena query history | Someone with query history access could reverse client IP hashes | Salt kept outside Git. Production: Secrets Manager or tokenisation at ingestion |
| R5 | One AWS identity runs dbt and manual queries, so people can run DML on pipeline tables | Silent data damage, as in RCA-002 | Ground rules in the runbooks. Production: separate pipeline and read-only roles |
| R6 | Iceberg snapshots are kept for about one day after weekly VACUUM | Older damage cannot be measured with time travel | Repairs replay from bronze, which works for any age (RB-06) |
| R7 | Alerts go only to Airflow task logs unless `ALERT_WEBHOOK_URL` is set | A failure can go unnoticed | Check DAG runs after each session. Production: route alerts to chat or paging |

## Assumptions

| ID | Assumption | If wrong |
|---|---|---|
| A1 | Synthetic events are representative enough of gateway telemetry (volume, duplicates, late arrivals) | Thresholds and performance numbers would need re-tuning on real data |
| A2 | `event_id` is unique per real event and stays the same when a producer retries | Silver deduplication would miss copies |
| A3 | The 2-hour lookback is longer than the worst gap between a row's ingest time and its commit | Rows could be missed. The completeness test is the safety net |
| A4 | The PII salt never changes | Hashes for the same IP would differ before and after the change |
| A5 | Device attributes are static, so Type 1 dimensions are enough | History would need an SCD Type 2 snapshot |
| A6 | All timestamps are UTC | Hourly facts would shift |

## Issues (open)

| ID | Issue | Workaround or next step |
|---|---|---|
| I1 | DLQ replay is not automated | Manual triage with RB-02. Next step: a replay job that re-publishes by header |
| I2 | Consumer lag and DLQ volume are not monitored from Airflow (Kafka runs on a separate Docker network) | Bronze freshness is the indirect alarm. Production: MSK or CloudWatch metrics |
| I3 | No value-level reconciliation: a change that stays within valid values passes every test (RCA-002) | Proposed: hourly bronze, silver and gold reconciliation plus a per-hour checksum |
| I4 | CI parses dbt but does not build it against AWS, and has no Airflow DAG import test | Production: OIDC role and `dbt build --select state:modified+` in CI |
| I5 | The schema gate checks each contract version against the previous one, but does not stop someone editing a version that is already registered | Treat contract files as immutable. Next step: fail CI when an existing `*_vN.avsc` changes |

Resolved issues are in the README's Lessons learned table and in [RCA-001](rca/RCA-001-merge-write-amplification.md) and [RCA-002](rca/RCA-002-silver-damaged-during-duplicate-incident.md).

## Dependencies

| ID | Dependency | Version or setting | Note |
|---|---|---|---|
| D1 | Amazon S3, AWS Glue Data Catalog, Amazon Athena | eu-north-1, Free plan | No MSK, MWAA or EC2, by design for cost |
| D2 | Kafka and Schema Registry images | `apache/kafka:4.1.1`, `confluentinc/cp-schema-registry:7.7.1` | Also used by the CI schema gate |
| D3 | Kafka UI image | `ghcr.io/kafbat/kafka-ui:latest` | Not pinned, so a new release could change behaviour |
| D4 | Airflow | Astro Runtime 3.3-12, astronomer-cosmos >=1.10,<2, Amazon provider | dbt runs in its own venv inside the image |
| D5 | dbt | dbt-core 1.12, dbt-athena 1.11 | dbt v2 (Fusion) has no Athena adapter |
| D6 | Python libraries | confluent-kafka 2.x, pyiceberg 0.10 to 0.11 | Ranges in the requirements files |
| D7 | GitHub Actions | `actions/checkout@v6`, `actions/setup-python@v6` | Free for public repositories |