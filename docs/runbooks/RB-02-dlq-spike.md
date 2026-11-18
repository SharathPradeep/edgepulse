# RB-02: DLQ spike

| | |
|---|---|
| Symptom | Message count on `gateway.dlq` rising in Kafka UI, or consumer logs show `Flushing batch: N good records, M sent to DLQ` with M above 0 |
| Alert | None directly. The DLQ is not monitored from Airflow |
| Data at risk | Only the rejected messages. Every other message still reaches bronze |

## What it means

The consumer sends a message to the dead-letter queue when it cannot decode it or when it fails validation (a missing required field or an event time more than 5 minutes in the future). Nothing is dropped silently: the original message is kept with headers explaining why.

## Check

1. **Kafka UI > Topics > `gateway.dlq` > Messages.** Open a few recent messages and read the headers:

   | Header | Meaning |
   |---|---|
   | `error` | Why it was rejected |
   | `source_topic`, `source_partition`, `source_offset` | Where the original message came from |
   | `failed_at` | When it was rejected (UTC) |

2. **Are all rejections the same error, from one topic?** One error from one source usually means one faulty producer.

## Fix by error

| `error` header | Cause | Fix |
|---|---|---|
| `deserialisation failed: ... Invalid magic byte` | A producer wrote data that is not registry-framed Avro (a poison message) | Find and fix that producer. These messages cannot be replayed as they are |
| `deserialisation failed: ...` mentioning a schema that cannot be found | The registry lost its schemas, for example after `docker compose down` removed Kafka | Re-register contracts with `python contracts/register_schemas.py`, then fix the producer |
| `missing event_id`, `missing device_id`, `missing tenant_id` or `missing event_ts` | Producer sending incomplete events | Fix the producer |
| `event_ts more than 5 minutes in the future` | Device clock skew | Fix the device or producer clock. Agree with the data owner whether such events should be accepted |

## Replay

Not automated in this project (known gap). Once the cause is fixed, a replay job would read `gateway.dlq`, select messages by header and re-publish the original value to `source_topic`. Replay is safe because silver deduplicates on `event_id`. Poison messages that are not valid Avro cannot be replayed and stay in the DLQ as the audit record.

## Verify

- The DLQ count stops rising.
- Consumer flush lines show `0 sent to DLQ`.

## In production

Alert on the DLQ message rate from MSK or CloudWatch metrics and build the replay job above.