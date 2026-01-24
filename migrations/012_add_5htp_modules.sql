-- Migration 012: Add 5-HTP Standalone Modules
-- Date: 2025-01-25
-- Product: 5-HTP Capsules (Supliful handle: 5-htp-capsules)
-- SKU: VOX45HTP
-- Module Codes: NEU-5HTP-F-105 (MAXima²), NEU-5HTP-M-105 (MAXimo²)

-- ============================================================
-- 5-HTP (5-Hydroxytryptophan) - Griffonia simplicifolia
-- ============================================================
-- Evidence Tier: TIER 2 (contextual evidence)
-- OS Layer: Support (targeted mood/sleep support)
-- Biological Domain: Neurotransmission & Cognitive Circuits
-- 
-- Clinical Evidence:
-- - Serotonin precursor with documented effects on mood and sleep
-- - Multiple RCTs for depression and sleep quality
-- - Well-established safety profile at standard doses (50-300mg)
--
-- Contraindications:
-- - Do not combine with SSRIs/SNRIs/MAOIs (serotonin syndrome risk)
-- - Avoid in pregnancy/breastfeeding
-- - Caution with carbidopa (may cause scleroderma-like illness)
-- ============================================================

BEGIN;

-- Insert MAXima² module (Female)
INSERT INTO os_modules_v3_1 (
    module_code,
    shopify_handle,
    product_name,
    product_link,
    os_environment,
    os_layer,
    net_quantity,
    net_quantity_label,
    front_label_text,
    back_label_text,
    fda_disclaimer,
    supliful_handle,
    biological_domain,
    disclaimer_applicability,
    disclaimer_symbol,
    tier,
    supplier_status,
    is_launch_v1,
    created_at,
    updated_at
)
VALUES (
    'NEU-5HTP-F-105',
    '5-htp-capsules-maxima',
    '5-HTP Capsules',
    'https://supliful.com/catalog/5-htp-capsules',
    'MAXima²',
    'Support',
    '60 capsules',
    '60 capsules / 50mg per capsule',
    E'GenoMAX² | MAXima²\n5-HTP Capsules\nSupport System Module\n60 capsules',
    E'GenoMAX² - OS Module\n\nSupplement Facts / Ingredients:\n- 5-HTP (5-Hydroxytryptophan) from Griffonia simplicifolia seed extract - 50mg per capsule\n\nSuggested Use:\nAs a dietary supplement, take one (1) capsule daily, preferably in the evening or as directed by your healthcare professional. Do not exceed 200mg per day without medical supervision.\n\nSafety Notes: Good (TIER 2)\n- Well-tolerated at doses up to 300mg/day\n- Common side effects: mild GI discomfort, nausea (usually transient)\n\nContraindications:\n- Do NOT combine with SSRIs, SNRIs, MAOIs, or other serotonergic medications (risk of serotonin syndrome)\n- Avoid during pregnancy and breastfeeding\n- Use caution with carbidopa\n- Discontinue 2 weeks before surgery\n\n*These statements have not been evaluated by the Food and Drug Administration. This product is not intended to diagnose, treat, cure, or prevent any disease.',
    'This statement has not been evaluated by the Food and Drug Administration. This product is not intended to diagnose, treat, cure, or prevent any disease.',
    '5-htp-capsules',
    'Neurotransmission & Cognitive Circuits',
    'SUPPLEMENT',
    '*',
    'TIER 2',
    'ACTIVE',
    TRUE,
    NOW(),
    NOW()
)
ON CONFLICT (module_code) DO UPDATE SET
    updated_at = NOW(),
    supliful_handle = EXCLUDED.supliful_handle,
    supplier_status = EXCLUDED.supplier_status;

-- Insert MAXimo² module (Male)
INSERT INTO os_modules_v3_1 (
    module_code,
    shopify_handle,
    product_name,
    product_link,
    os_environment,
    os_layer,
    net_quantity,
    net_quantity_label,
    front_label_text,
    back_label_text,
    fda_disclaimer,
    supliful_handle,
    biological_domain,
    disclaimer_applicability,
    disclaimer_symbol,
    tier,
    supplier_status,
    is_launch_v1,
    created_at,
    updated_at
)
VALUES (
    'NEU-5HTP-M-105',
    '5-htp-capsules-maximo',
    '5-HTP Capsules',
    'https://supliful.com/catalog/5-htp-capsules',
    'MAXimo²',
    'Support',
    '60 capsules',
    '60 capsules / 50mg per capsule',
    E'GenoMAX² | MAXimo²\n5-HTP Capsules\nSupport System Module\n60 capsules',
    E'GenoMAX² - OS Module\n\nSupplement Facts / Ingredients:\n- 5-HTP (5-Hydroxytryptophan) from Griffonia simplicifolia seed extract - 50mg per capsule\n\nSuggested Use:\nAs a dietary supplement, take one (1) capsule daily, preferably in the evening or as directed by your healthcare professional. Do not exceed 200mg per day without medical supervision.\n\nSafety Notes: Good (TIER 2)\n- Well-tolerated at doses up to 300mg/day\n- Common side effects: mild GI discomfort, nausea (usually transient)\n\nContraindications:\n- Do NOT combine with SSRIs, SNRIs, MAOIs, or other serotonergic medications (risk of serotonin syndrome)\n- Avoid during pregnancy and breastfeeding\n- Use caution with carbidopa\n- Discontinue 2 weeks before surgery\n\n*These statements have not been evaluated by the Food and Drug Administration. This product is not intended to diagnose, treat, cure, or prevent any disease.',
    'This statement has not been evaluated by the Food and Drug Administration. This product is not intended to diagnose, treat, cure, or prevent any disease.',
    '5-htp-capsules',
    'Neurotransmission & Cognitive Circuits',
    'SUPPLEMENT',
    '*',
    'TIER 2',
    'ACTIVE',
    TRUE,
    NOW(),
    NOW()
)
ON CONFLICT (module_code) DO UPDATE SET
    updated_at = NOW(),
    supliful_handle = EXCLUDED.supliful_handle,
    supplier_status = EXCLUDED.supplier_status;

-- Log to audit
INSERT INTO audit_log (entity_type, entity_id, action, metadata, created_at)
VALUES 
    ('os_module', NULL, 'module_created', '{"module_code": "NEU-5HTP-F-105", "product_name": "5-HTP Capsules", "environment": "MAXima²", "tier": "TIER 2", "migration": "012_add_5htp_modules"}', NOW()),
    ('os_module', NULL, 'module_created', '{"module_code": "NEU-5HTP-M-105", "product_name": "5-HTP Capsules", "environment": "MAXimo²", "tier": "TIER 2", "migration": "012_add_5htp_modules"}', NOW());

COMMIT;

-- Verification queries
SELECT module_code, product_name, os_environment, os_layer, tier, supplier_status, supliful_handle
FROM os_modules_v3_1 
WHERE module_code LIKE 'NEU-5HTP%'
ORDER BY module_code;
