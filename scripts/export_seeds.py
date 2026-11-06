"""Export the generator's reference data as dbt seed CSVs, so dimensions match the events exactly."""
import csv
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "generator"))
from registry import DEVICES, WAF_RULES  # noqa: E402

SEEDS = ROOT / "airflow" / "include" / "dbt" / "seeds"
SEEDS.mkdir(parents=True, exist_ok=True)

with open(SEEDS / "device_registry.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=["device_id", "tenant_id", "product_version", "region"])
    writer.writeheader()
    writer.writerows(DEVICES)

with open(SEEDS / "waf_rules.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["rule_id", "attack_category"])
    writer.writerows(WAF_RULES.items())

STATUS = {200: "OK", 201: "Created", 204: "No Content", 301: "Moved Permanently", 400: "Bad Request",
          401: "Unauthorized", 403: "Forbidden", 404: "Not Found", 500: "Internal Server Error",
          502: "Bad Gateway", 503: "Service Unavailable"}
with open(SEEDS / "http_status_codes.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["status_code", "status_text", "status_class", "is_error"])
    for code, text in STATUS.items():
        writer.writerow([code, text, f"{code // 100}xx", str(code >= 400).lower()])

print(f"Wrote {len(DEVICES)} devices, {len(WAF_RULES)} rules and {len(STATUS)} status codes to {SEEDS}")
