"""
GenoMAX² Excel Override Module v1
Implements Option D: Primary ingredient for classification + Aggregate safety/evidence

FLOW:
1. Load Excel with is_primary_ingredient column
2. Build normalized payload (1 row per SKU+env)
3. Map payload to DB modules via shopify_handle
4. Dry-run diff report
5. Execute override in single transaction
6. QC verification
"""

import os
import json
import uuid
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
from fastapi import APIRouter, HTTPException, UploadFile, File
import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd
from io import BytesIO

router = APIRouter(prefix="/api/v1/catalog", tags=["Catalog Override"])

DATABASE_URL = os.getenv("DATABASE_URL")


def get_db():
    try:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        return None


# ============================================================================
# CONSTANTS
# ============================================================================

EXCEL_SKU_TO_DB_BASE = {
    "appetite-balance-weight-support-strips": "appetite-balance-weight-support-strips",
    "beetroot-capsules": "beetroot-capsules",
    "diet-drops-ultra": "diet-drops-ultra",
    "energy-powder-cotton-candy": "energy-powder-cotton-candy",
    "focus-powder-sour-candy": "focus-powder-sour-candy",
    "iron-strips": "iron-strips",
    "l-glutamine-powder": "l-glutamine-powder",
    "multivitamin-bear-gummies-adult": "multivitamin-bear-gummies-adult",
    "mushroom-coffee-fusion-lions-mane-chaga-16oz": "mushroom-coffee-fusion-lions-mane-chaga-16oz",
    "ashwagandha-capsules": "ashwagandha",
    "berberine-capsules": "berberine",
    "cognitive-support-capsules": "cognitive-support",
    "colon-gentle-cleanse-sachets": "colon-gentle-cleanse",
    "complete-multivitamin-capsules": "complete-multivitamin",
    "coq10-ubiquinone-capsules": "coq10-ubiquinone",
    "creatine-monohydrate-powder": "creatine-monohydrate",
    "joint-support-capsules": "joint-support",
    "keto-5-capsules": "keto-5",
    "liver-support-capsules": "liver-support",
    "maca-plus-capsules": "maca-plus",
    "magnesium-glycinate-capsules": "magnesium-glycinate",
    "nitric-oxide-capsules": "nitric-oxide",
    "platinum-turmeric-capsules": "platinum-turmeric",
    "sleep-formula-capsules": "sleep-formula",
    "vision-support-capsules": "vision-support",
    "omega-3-epa-dha-softgel-capsules": "omega-3-epa-180mg-dha-120mg",
    "vitamin-d3-2000iu-softgel-capsules": "vitamin-d3-2-000-iu",
    "nad-plus-capsules": "nad",
    "probiotic-40-billion-prebiotics-capsules": "probiotic-40-billion-with-prebiotics",
    "resveratrol-50-percent-capsules": "resveratrol-50-600mg",
    "max-detox-acai-capsules": "max-detox-acai-detox",
    "mens-vitality-tablets": "men-s-vitality",
}

EXCEL_PRODUCTS_NOT_IN_DB = {
    "kojic-acid-turmeric-soap",
    "moisturizing-strengthening-hair-oil-old",
    "green-tea-antioxidant-serum",
    "vitamin-glow-serum",
    "vitamin-c-serum",
    "recovery-cream",
    "peptide-hair-growth-serum",
}

GENDER_SPECIFIC_EXCLUSIONS = {
    ("mens-vitality-tablets", "MAXima²"),
}

# Primary ingredient mapping for multi-ingredient products
PRIMARY_INGREDIENT_MAP = {
    'cognitive-support-capsules': 'Bacopa monnieri',
    'complete-multivitamin-capsules': 'Vitamin B12 Methylcobalamin',
    'coq10-ubiquinone-capsules': 'CoQ10 Ubiquinone/Ubiquinol',
    'energy-powder-cotton-candy': 'Spirulina',
    'liver-support-capsules': 'NAC N-Acetyl Cysteine',
    'nitric-oxide-capsules': 'Citrulline',
    'vision-support-capsules': 'Zinc',
    'vitamin-d3-2000iu-softgel-capsules': 'Vitamin D3',
}

# Fields to override from Excel
OVERRIDE_FIELDS = [
    'os_layer',
    'biological_domain',  # mapped from biological_subsystem
    'suggested_use_full',  # mapped from suggested_use
    'safety_notes',
    'contraindications',
    'dosing_protocol',  # mapped from dosage_context_note
    'evidence_rationale',
]


def get_expected_handle(sku: str, env: str) -> Optional[str]:
    """Get expected DB shopify_handle from Excel SKU and environment."""
    if (sku, env) in GENDER_SPECIFIC_EXCLUSIONS:
        return None
    if sku in EXCEL_PRODUCTS_NOT_IN_DB:
        return None
    
    base = EXCEL_SKU_TO_DB_BASE.get(sku, sku)
    if env == "MAXimo²":
        return f"{base}-maximo"
    elif env == "MAXima²":
        return f"{base}-maxima"
    return base


def build_payload_from_excel(df: pd.DataFrame) -> Tuple[List[Dict], List[Dict]]:
    """
    Build normalized module-level payload from Excel DataFrame.
    Returns (payload_records, violations)
    """
    # Filter to YES rows
    yes_rows = df[df['selected_as_module'].astype(str).str.upper().str.strip() == 'YES'].copy()
    
    if 'is_primary_ingredient' not in yes_rows.columns:
        # Auto-assign primary ingredients
        yes_rows['is_primary_ingredient'] = False
        for idx, row in yes_rows.iterrows():
            sku = row.get('supliful_sku', '')
            ingredient = row.get('research_ingredient', '')
            if sku in PRIMARY_INGREDIENT_MAP:
                if ingredient == PRIMARY_INGREDIENT_MAP[sku]:
                    yes_rows.at[idx, 'is_primary_ingredient'] = True
            else:
                # Single-ingredient products
                yes_rows.at[idx, 'is_primary_ingredient'] = True
    
    payload_records = []
    violations = []
    
    for (sku, env), group in yes_rows.groupby(['supliful_sku', 'os_environment']):
        primary_rows = group[group['is_primary_ingredient'] == True]
        
        if len(primary_rows) != 1:
            violations.append({
                'supliful_sku': sku,
                'os_environment': env,
                'primary_count': len(primary_rows),
                'yes_row_count': len(group)
            })
            continue
        
        primary = primary_rows.iloc[0]
        
        # Aggregate safety fields from all ingredients
        safety_notes_list = group['safety_notes'].dropna().unique().tolist()
        contraindications_list = group['contraindications'].dropna().unique().tolist()
        
        # Evidence rationale with ingredient labels
        evidence_parts = []
        for _, row in group.iterrows():
            ing = row.get('research_ingredient', 'Unknown')
            ev = row.get('evidence_rationale', '')
            if ev and str(ev).strip():
                evidence_parts.append(f"- {ing}: {ev}")
        
        payload_records.append({
            'supliful_sku': sku,
            'os_environment': env,
            'supliful_product_name': primary.get('supliful_product_name', ''),
            'tier': primary.get('tier', ''),
            'os_layer': primary.get('os_layer', ''),
            'biological_subsystem': primary.get('biological_subsystem', ''),
            'suggested_use': primary.get('suggested_use', ''),
            'safety_notes': '\n'.join([str(s) for s in safety_notes_list if s and str(s).strip()]),
            'contraindications': '\n'.join([str(c) for c in contraindications_list if c and str(c).strip()]),
            'dosage_context_note': primary.get('dosage_context_note', ''),
            'evidence_rationale': '\n'.join(evidence_parts),
            'primary_ingredient': primary.get('research_ingredient', ''),
            'all_ingredients': ', '.join(group['research_ingredient'].dropna().tolist())
        })
    
    return payload_records, violations


@router.post("/override/preflight")
async def override_preflight(file: UploadFile = File(...)) -> Dict[str, Any]:
    """
    Pre-flight check for Excel override.
    Validates Excel structure and builds normalized payload.
    """
    try:
        contents = await file.read()
        df = pd.read_excel(BytesIO(contents), sheet_name='Sheet1')
        
        # Check required columns
        required_cols = [
            'research_ingredient', 'supliful_product_name', 'supliful_sku', 
            'selected_as_module', 'tier', 'os_layer', 'biological_subsystem', 
            'os_environment', 'suggested_use', 'safety_notes', 'contraindications',
            'dosage_context_note', 'evidence_rationale'
        ]
        missing_cols = [c for c in required_cols if c not in df.columns]
        
        if missing_cols:
            return {
                "status": "FAIL",
                "reason": f"Missing columns: {missing_cols}",
                "available_columns": list(df.columns)
            }
        
        # Build payload
        payload, violations = build_payload_from_excel(df)
        
        # Count YES rows
        yes_rows = df[df['selected_as_module'].astype(str).str.upper().str.strip() == 'YES']
        unique_keys = len(yes_rows.groupby(['supliful_sku', 'os_environment']))
        
        # Check mappability
        mappable = []
        not_in_db = []
        gender_excluded = []
        
        for record in payload:
            sku = record['supliful_sku']
            env = record['os_environment']
            handle = get_expected_handle(sku, env)
            
            if (sku, env) in GENDER_SPECIFIC_EXCLUSIONS:
                gender_excluded.append({'sku': sku, 'env': env})
            elif handle is None:
                not_in_db.append({'sku': sku, 'env': env})
            else:
                mappable.append({
                    'sku': sku, 
                    'env': env, 
                    'handle': handle,
                    'os_layer': record['os_layer'],
                    'biological_subsystem': record['biological_subsystem']
                })
        
        return {
            "status": "PASS" if len(violations) == 0 else "FAIL",
            "excel_stats": {
                "total_rows": len(df),
                "yes_rows": len(yes_rows),
                "unique_sku_env_pairs": unique_keys
            },
            "payload_stats": {
                "payload_records": len(payload),
                "violations": len(violations),
                "mappable_to_db": len(mappable),
                "not_in_db": len(not_in_db),
                "gender_excluded": len(gender_excluded)
            },
            "violations": violations[:10] if violations else [],
            "not_in_db_sample": not_in_db[:10],
            "gender_excluded": gender_excluded,
            "mappable_sample": mappable[:10]
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Preflight error: {str(e)}")


@router.post("/override/dry-run")
async def override_dry_run(file: UploadFile = File(...)) -> Dict[str, Any]:
    """
    Dry-run override: compute diffs without making changes.
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        contents = await file.read()
        df = pd.read_excel(BytesIO(contents), sheet_name='Sheet1')
        
        payload, violations = build_payload_from_excel(df)
        
        if violations:
            return {
                "status": "BLOCKED",
                "reason": "Primary ingredient violations exist",
                "violations": violations
            }
        
        cur = conn.cursor()
        
        # Get current DB values
        cur.execute("""
            SELECT module_code, shopify_handle, os_environment, 
                   os_layer, biological_domain, suggested_use_full,
                   safety_notes, contraindications, dosing_protocol,
                   evidence_rationale
            FROM os_modules_v3_1
            WHERE supplier_status IS NULL 
               OR supplier_status NOT IN ('DUPLICATE_INACTIVE')
        """)
        db_rows = cur.fetchall()
        
        db_by_handle = {}
        for row in db_rows:
            key = (row['shopify_handle'], row['os_environment'])
            db_by_handle[key] = dict(row)
        
        # Compute diffs
        diffs = []
        field_change_counts = {f: 0 for f in OVERRIDE_FIELDS}
        matched = 0
        not_found = []
        
        for record in payload:
            sku = record['supliful_sku']
            env = record['os_environment']
            handle = get_expected_handle(sku, env)
            
            if handle is None:
                continue
            
            key = (handle, env)
            if key not in db_by_handle:
                not_found.append({'sku': sku, 'env': env, 'handle': handle})
                continue
            
            matched += 1
            db_row = db_by_handle[key]
            
            # Field mappings: Excel -> DB
            field_mappings = {
                'os_layer': ('os_layer', record.get('os_layer', '')),
                'biological_domain': ('biological_subsystem', record.get('biological_subsystem', '')),
                'suggested_use_full': ('suggested_use', record.get('suggested_use', '')),
                'safety_notes': ('safety_notes', record.get('safety_notes', '')),
                'contraindications': ('contraindications', record.get('contraindications', '')),
                'dosing_protocol': ('dosage_context_note', record.get('dosage_context_note', '')),
                'evidence_rationale': ('evidence_rationale', record.get('evidence_rationale', '')),
            }
            
            record_diffs = []
            for db_field, (excel_source, excel_value) in field_mappings.items():
                db_value = db_row.get(db_field) or ''
                excel_value = str(excel_value) if excel_value else ''
                
                if db_value.strip() != excel_value.strip():
                    record_diffs.append({
                        'field': db_field,
                        'db_value': db_value[:100] + '...' if len(str(db_value)) > 100 else db_value,
                        'excel_value': excel_value[:100] + '...' if len(excel_value) > 100 else excel_value
                    })
                    field_change_counts[db_field] += 1
            
            if record_diffs:
                diffs.append({
                    'sku': sku,
                    'env': env,
                    'handle': handle,
                    'module_code': db_row['module_code'],
                    'diffs': record_diffs
                })
        
        cur.close()
        conn.close()
        
        return {
            "status": "DRY_RUN_COMPLETE",
            "summary": {
                "payload_records": len(payload),
                "matched_in_db": matched,
                "not_found_in_db": len(not_found),
                "records_with_diffs": len(diffs),
                "total_field_changes": sum(field_change_counts.values())
            },
            "field_change_counts": field_change_counts,
            "not_found": not_found[:10],
            "diff_samples": diffs[:20]
        }
        
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Dry-run error: {str(e)}")


@router.post("/override/execute")
async def override_execute(file: UploadFile = File(...), confirm: bool = False) -> Dict[str, Any]:
    """
    Execute the override. Requires confirm=True to proceed.
    """
    if not confirm:
        return {
            "status": "BLOCKED",
            "message": "Set confirm=True to execute override"
        }
    
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        contents = await file.read()
        df = pd.read_excel(BytesIO(contents), sheet_name='Sheet1')
        
        payload, violations = build_payload_from_excel(df)
        
        if violations:
            return {
                "status": "BLOCKED",
                "reason": "Primary ingredient violations exist",
                "violations": violations
            }
        
        batch_id = str(uuid.uuid4())
        cur = conn.cursor()
        
        # Start transaction
        updated_count = 0
        field_updates = {f: 0 for f in OVERRIDE_FIELDS}
        errors = []
        
        for record in payload:
            sku = record['supliful_sku']
            env = record['os_environment']
            handle = get_expected_handle(sku, env)
            
            if handle is None:
                continue
            
            try:
                # Get current values for logging
                cur.execute("""
                    SELECT module_code, os_layer, biological_domain, 
                           suggested_use_full, safety_notes, contraindications,
                           dosing_protocol, evidence_rationale
                    FROM os_modules_v3_1
                    WHERE shopify_handle = %s AND os_environment = %s
                """, (handle, env))
                
                existing = cur.fetchone()
                if not existing:
                    continue
                
                module_code = existing['module_code']
                
                # Build update and log changes
                updates = []
                params = []
                
                field_mappings = [
                    ('os_layer', record.get('os_layer', '')),
                    ('biological_domain', record.get('biological_subsystem', '')),
                    ('suggested_use_full', record.get('suggested_use', '')),
                    ('safety_notes', record.get('safety_notes', '')),
                    ('contraindications', record.get('contraindications', '')),
                    ('dosing_protocol', record.get('dosage_context_note', '')),
                    ('evidence_rationale', record.get('evidence_rationale', '')),
                ]
                
                for db_field, excel_value in field_mappings:
                    old_value = existing.get(db_field) or ''
                    new_value = str(excel_value) if excel_value else ''
                    
                    if old_value.strip() != new_value.strip():
                        updates.append(f"{db_field} = %s")
                        params.append(new_value)
                        field_updates[db_field] += 1
                        
                        # Log the change
                        cur.execute("""
                            INSERT INTO catalog_override_log_v1 
                            (override_batch_id, module_code, os_environment, 
                             field_name, old_value, new_value, source_sku)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """, (batch_id, module_code, env, db_field, 
                              old_value[:1000] if old_value else None, 
                              new_value[:1000] if new_value else None, sku))
                
                if updates:
                    updates.append("updated_at = NOW()")
                    sql = f"""
                        UPDATE os_modules_v3_1 
                        SET {', '.join(updates)}
                        WHERE shopify_handle = %s AND os_environment = %s
                    """
                    params.extend([handle, env])
                    cur.execute(sql, params)
                    updated_count += 1
                    
            except Exception as e:
                errors.append({
                    'sku': sku,
                    'env': env,
                    'error': str(e)
                })
        
        conn.commit()
        cur.close()
        conn.close()
        
        return {
            "status": "SUCCESS",
            "batch_id": batch_id,
            "summary": {
                "payload_records": len(payload),
                "modules_updated": updated_count,
                "errors": len(errors)
            },
            "field_updates": field_updates,
            "errors": errors[:10] if errors else []
        }
        
    except Exception as e:
        try:
            conn.rollback()
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Execute error: {str(e)}")


@router.get("/override/logs/{batch_id}")
def get_override_logs(batch_id: str) -> Dict[str, Any]:
    """Get logs for a specific override batch."""
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT module_code, os_environment, field_name, 
                   old_value, new_value, source_sku, created_at
            FROM catalog_override_log_v1
            WHERE override_batch_id = %s
            ORDER BY created_at
        """, (batch_id,))
        
        rows = cur.fetchall()
        cur.close()
        conn.close()
        
        return {
            "batch_id": batch_id,
            "log_count": len(rows),
            "logs": [dict(r) for r in rows[:100]]
        }
        
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Log fetch error: {str(e)}")
