FROM astrocrpublic.azurecr.io/runtime:3.3-12

RUN python -m venv dbt_venv && \
    dbt_venv/bin/pip install --no-cache-dir "dbt-core>=1.12,<1.13" "dbt-athena>=1.11,<1.12"