   # ADR 002: Contracts registered from the repo, not by producers
   Status: Accepted
   Decision: Avro contracts live in contracts/ and are registered by a script with BACKWARD compatibility. Producers run with auto.register.schemas disabled.
   Consequences: Schema changes go through a pull request (and later a CI compatibility check). Producers cannot publish incompatible versions by accident.