# RB-04: Incompatible schema

| | |
|---|---|
| Symptoms | `python contracts/register_schemas.py` prints `INCOMPATIBLE` and exits with code 1. Or a producer fails because its schema is not registered. Or the consumer logs `records carry fields missing from the latest contract` and restarts. Or the DLQ fills with deserialisation errors |
| Rule | Every subject uses BACKWARD compatibility: a consumer on the new contract must be able to read every event written with the previous one |
| Data at risk | None. Unregistered changes cannot be published (`auto.register.schemas` is off), and the consumer fails the batch rather than dropping fields |

## What it means

A contract is the agreed shape of an event. Someone changed it in a way that old events or existing consumers cannot handle, or a producer is ahead of what has been registered.

## Check

1. **What exactly is incompatible?**
   ```bash
   python contracts/register_schemas.py --check contracts/<new_file>.avsc
   ```
   The message names the field and the problem, for example `READER_FIELD_MISSING_DEFAULT_VALUE`.
2. **Kafka UI > Schema Registry > subject.** Check the latest version and that compatibility is still `BACKWARD`.

## Fix

| Change wanted | Do this |
|---|---|
| Add a field | Make it optional with a default: `"type": ["null", "string"], "default": null`. This is how `tls_version` was added in v2 |
| Rename a field | Add the new name as optional, keep the old one, move consumers over, then deprecate the old field |
| Change a type or remove a required field | Breaking. Publish to a new versioned topic and subject, run both side by side and migrate consumers. For gold, ship a new model version as with `fct_request_hourly` v1 and v2 |

Never set the subject's compatibility to `NONE` to force a change through.

**Consumer stuck on unknown fields:** a producer is sending a field that no registered contract has, for example after the registry was reset. Register the correct contract, or roll the producer back. The consumer recovers on its next retry or restart, and no messages are lost because offsets were not committed.

## Rollout order for a compatible change

1. Register the new version: `python contracts/register_schemas.py`
2. Restart the consumer: `docker compose --profile pipeline up -d --build consumer`
3. Deploy the producer: `docker compose --profile generator up -d --build generator`
4. Add the column to the silver model. `append_new_columns` adds it to the table

## Verify

- `register_schemas.py` exits with code 0.
- The consumer logs `evolved ep_bronze.<table>: added [...]` for new fields.
- `dbt build --select <silver model>+` is green.

## In production

The same check runs in CI on every pull request that touches `contracts/`, so a breaking change never reaches main.