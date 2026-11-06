"""Every event the generator builds must be valid against its registered contract."""
import json
from pathlib import Path

import pytest
from fastavro import parse_schema
from fastavro.validation import validate

import generator
from registry import DEVICES

CONTRACTS = Path(__file__).resolve().parent.parent / "contracts"


@pytest.mark.parametrize("kind", list(generator.TOPICS))
def test_generated_events_match_contract(kind):
    _, file_name, _ = generator.TOPICS[kind]
    schema = parse_schema(json.loads((CONTRACTS / file_name).read_text()))
    for device in DEVICES[:50]:
        assert validate(generator.BUILDERS[kind](device), schema)


def test_device_registry_is_stable():
    assert len(DEVICES) == 200
    assert DEVICES[0]["device_id"] == "dev-0001"
    assert len({d["device_id"] for d in DEVICES}) == 200
