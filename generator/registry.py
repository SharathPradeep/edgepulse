"""Fixed reference data shared by the generator and (later) the dbt seeds.

A fixed random seed means the same 200 devices and WAF rules every run.
"""
import random

_rng = random.Random(42)

VERSIONS = ["16.1", "17.1", "17.5"]
REGIONS = ["eu-west", "eu-north", "us-east", "ap-south", "me-central"]

DEVICES = [
    {
        "device_id": f"dev-{i:04d}",
        "tenant_id": f"t-{_rng.randint(1, 10):02d}",
        "product_version": _rng.choice(VERSIONS),
        "region": _rng.choice(REGIONS),
    }
    for i in range(1, 201)
]

WAF_RULES = {
    "WAF-1001": "sqli",
    "WAF-1002": "sqli",
    "WAF-2001": "xss",
    "WAF-3001": "bot",
    "WAF-4001": "path_traversal",
    "WAF-5001": "rate_limit",
}
