"""Register the Avro event contracts in Schema Registry, oldest version first.

Contracts are registered here, not by producers (auto.register.schemas is off),
so a producer can never publish a new schema version by accident.

Usage:
    python contracts/register_schemas.py               register every version not yet registered
    python contracts/register_schemas.py --check FILE  test FILE against the latest version, register nothing

Every new version is checked against the latest registered version under
BACKWARD compatibility before it is registered. The script exits with code 1
if any contract is incompatible, which is what the CI gate relies on.
Uses only the standard library, so CI needs no extra packages to run it.
"""
import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.request
from pathlib import Path

SR_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081").rstrip("/")
HERE = Path(__file__).resolve().parent
CONTRACT_FILE = re.compile(r"^(?P<kind>[a-z_]+)_v(?P<version>\d+)\.avsc$")
HEADERS = {"Content-Type": "application/vnd.schemaregistry.v1+json"}


def call(method, path, body=None):
    """Call the Schema Registry REST API. Returns (HTTP status, parsed JSON body)."""
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(f"{SR_URL}{path}", data=data, method=method, headers=HEADERS)
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status, json.loads(response.read() or b"{}")
    except urllib.error.HTTPError as err:
        return err.code, json.loads(err.read() or b"{}")


def subject_for(path):
    """request_log_v2.avsc (or request_log_breaking.avsc) -> gateway.request_log-value"""
    return f"gateway.{path.stem.rsplit('_', 1)[0]}-value"


def check(subject, schema_str):
    """Return (compatible, messages) for schema_str against the latest version of subject."""
    status, body = call(
        "POST", f"/compatibility/subjects/{subject}/versions/latest?verbose=true", {"schema": schema_str}
    )
    if status == 404:                      # subject has no versions yet, so any first version is fine
        return True, []
    if status != 200:
        raise RuntimeError(f"compatibility check for {subject} failed: HTTP {status} {body}")
    return body["is_compatible"], body.get("messages", [])


def contract_files():
    """All versioned contracts, sorted so each subject's versions register in order."""
    files = [p for p in HERE.glob("*.avsc") if CONTRACT_FILE.match(p.name)]
    return sorted(files, key=lambda p: (CONTRACT_FILE.match(p.name)["kind"], int(CONTRACT_FILE.match(p.name)["version"])))


def register_all():
    failed_subjects = set()
    for path in contract_files():
        subject, schema_str = subject_for(path), path.read_text()
        if subject in failed_subjects:
            print(f"{path.name}: skipped, an earlier version of {subject} failed")
            continue

        call("PUT", f"/config/{subject}", {"compatibility": "BACKWARD"})
        status, body = call("POST", f"/subjects/{subject}", {"schema": schema_str})   # already registered?
        if status == 200:
            print(f"{path.name}: already registered as {subject} version {body['version']}")
            continue

        compatible, messages = check(subject, schema_str)
        if not compatible:
            print(f"{path.name}: INCOMPATIBLE with the latest {subject} version, not registered")
            for message in messages:
                print(f"    {message}")
            failed_subjects.add(subject)
            continue

        status, body = call("POST", f"/subjects/{subject}/versions", {"schema": schema_str})
        if status != 200:
            print(f"{path.name}: registration failed: HTTP {status} {body}")
            failed_subjects.add(subject)
            continue
        print(f"{path.name}: registered as {subject}, schema id {body['id']}, compatibility BACKWARD")
    return not failed_subjects


def check_file(path):
    subject = subject_for(path)
    compatible, messages = check(subject, path.read_text())
    print(f"{path.name} against the latest {subject}: {'compatible' if compatible else 'INCOMPATIBLE'}")
    for message in messages:
        print(f"    {message}")
    return compatible


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--check", type=Path, metavar="FILE", help="dry run: test one .avsc file, register nothing")
    args = parser.parse_args()
    ok = check_file(args.check) if args.check else register_all()
    sys.exit(0 if ok else 1)
