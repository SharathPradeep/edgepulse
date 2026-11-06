import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
for folder in ("ingestion", "generator"):
    sys.path.insert(0, str(ROOT / folder))
