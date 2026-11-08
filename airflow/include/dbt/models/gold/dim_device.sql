select device_id, tenant_id, product_version, region
from {{ ref('device_registry') }}