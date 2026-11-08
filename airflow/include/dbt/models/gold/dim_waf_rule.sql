select rule_id, attack_category
from {{ ref('waf_rules') }}