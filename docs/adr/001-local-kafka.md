# ADR 001: Run Kafka locally in Docker
Status: Accepted
Context: Amazon MSK is billed hourly and isn't available on the AWS Free plan.
Decision: Single-node Kafka 4.x (KRaft) with Schema Registry in Docker Compose.
Consequences: Free and fast to iterate. Production would use MSK or Confluent Cloud with only connection and auth settings changing.