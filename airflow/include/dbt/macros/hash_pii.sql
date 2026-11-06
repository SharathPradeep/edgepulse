{% macro hash_pii(column) -%}
lower(to_hex(sha256(to_utf8('{{ env_var("EP_PII_SALT") }}' || {{ column }}))))
{%- endmacro %}