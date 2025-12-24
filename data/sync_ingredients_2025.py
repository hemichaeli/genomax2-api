#!/usr/bin/env python3
"""
GenoMAX² Ingredient Database Sync Script
Version: 2025-12-24
Purpose: Sync audited ingredient dataset into production database

This script:
1. Extends the database schema with new columns
2. Loads the new GENOMAX2_FULL_MERGED_2025-12-24.csv dataset
3. Performs intelligent upserts (update existing, insert new)
4. Validates data integrity before commit
5. Reports detailed sync results
"""

import os
import csv
import json
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
import pandas as pd
from sqlalchemy import create_engine, text

# Database connection
DB_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:1!Qaz2wsx@localhost:5432/genomax2')
engine = create_engine(DB_URL)

# Script directory
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_FILE = os.path.join(SCRIPT_DIR, 'GENOMAX2_FULL_MERGED_2025-12-24.csv')

# Tier mapping
TIER_MAP = {
    '1': 'TIER_1_OS_CORE',
    '2': 'TIER_2_CONTEXTUAL', 
    '3': 'TIER_3_EXPLORATORY',
    1: 'TIER_1_OS_CORE',
    2: 'TIER_2_CONTEXTUAL',
    3: 'TIER_3_EXPLORATORY',
    1.0: 'TIER_1_OS_CORE',
    2.0: 'TIER_2_CONTEXTUAL',
    3.0: 'TIER_3_EXPLORATORY',
}

# Core 22 biomarkers for validation
CORE_BIOMARKERS = [
    'Triglycerides', 'CRP', 'HDL-C', 'LDL-C', 'HbA1c', 'Fasting glucose',
    '25-hydroxyvitamin D', 'RBC magnesium', 'Serum Mg', 'Serum zinc',
    'RBC zinc', 'Serum B12', 'MMA', 'Homocysteine', 'Iron', 'Ferritin',
    'TIBC', 'Transferrin saturation', 'TSH', 'Free T4', 'Free T3',
    'Cortisol', 'PTH', 'Calcium', 'Omega-3 index', 'EPA', 'DHA',
    'CoQ10 plasma levels', 'ALT', 'AST', 'GGT', 'Creatinine', 'eGFR',
    'Testosterone', 'Estradiol', 'Progesterone', 'FSH', 'LH'
]


def safe_int(val) -> Optional[int]:
    """Safely convert to integer."""
    if pd.isna(val) or val == '' or val is None:
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None


def safe_float(val) -> Optional[float]:
    """Safely convert to float."""
    if pd.isna(val) or val == '' or val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def safe_str(val, max_len: Optional[int] = None) -> Optional[str]:
    """Safely convert to string."""
    if pd.isna(val) or val is None:
        return None
    s = str(val).strip()
    if not s or s.lower() == 'nan':
        return None
    return s[:max_len] if max_len else s


def safe_bool(val) -> bool:
    """Safely convert to boolean."""
    if pd.isna(val) or val is None or val == '':
        return False
    if isinstance(val, bool):
        return val
    if isinstance(val, str):
        return val.upper() in ('YES', 'TRUE', '1', 'Y')
    return bool(val)


def normalize_tier(val) -> str:
    """Normalize tier value to standard enum."""
    if pd.isna(val) or val is None or val == '':
        return 'TIER_2_CONTEXTUAL'  # Default
    key = str(val).strip()
    if key in TIER_MAP:
        return TIER_MAP[key]
    try:
        num_key = float(key)
        if num_key in TIER_MAP:
            return TIER_MAP[num_key]
    except ValueError:
        pass
    return 'TIER_2_CONTEXTUAL'


def generate_sku(category: str, name: str, index: int) -> str:
    """Generate unique SKU for ingredient."""
    cat = re.sub(r'[^A-Z]', '', str(category).upper()[:3]) or 'GEN'
    nm = re.sub(r'[^A-Z]', '', str(name).upper()[:3]) or 'UNK'
    return f"GENO-{cat}-{nm}-{index:03d}"


def parse_percentage(val) -> Optional[str]:
    """Parse percentage values like '25%' or '<5%'."""
    if pd.isna(val) or val is None:
        return None
    s = str(val).strip()
    if not s or s.lower() == 'nan':
        return None
    return s


def validate_pmid(val) -> Optional[int]:
    """Validate and extract PMID (must be numeric)."""
    if pd.isna(val) or val is None:
        return None
    try:
        pmid = int(float(val))
        if 1000000 <= pmid <= 99999999:  # Reasonable PMID range
            return pmid
        return None
    except (ValueError, TypeError):
        return None


def validate_biomarkers(biomarkers_str: str) -> Tuple[List[str], List[str]]:
    """Validate biomarkers against core list. Returns (valid, unknown)."""
    if not biomarkers_str:
        return [], []
    
    biomarkers = [b.strip() for b in biomarkers_str.split(';')]
    valid = []
    unknown = []
    
    for bio in biomarkers:
        if not bio:
            continue
        # Check exact match or partial match
        matched = False
        for core in CORE_BIOMARKERS:
            if bio.lower() == core.lower() or bio.lower() in core.lower() or core.lower() in bio.lower():
                valid.append(bio)
                matched = True
                break
        if not matched:
            unknown.append(bio)
    
    return valid, unknown


def extend_schema():
    """Extend database schema with new columns for the audit dataset."""
    print("\n📊 Extending database schema...")
    
    new_columns = [
        ("tier_validated", "VARCHAR(30)"),
        ("validation_status", "VARCHAR(50)"),
        ("validation_notes", "TEXT"),
        ("effect_primary", "TEXT"),
        ("effect_size_value", "DECIMAL(5,2)"),
        ("effect_ci_lower", "DECIMAL(5,2)"),
        ("effect_ci_upper", "DECIMAL(5,2)"),
        ("secondary_effects", "TEXT"),
        ("clinical_measures", "TEXT"),
        ("dosage_optimal", "VARCHAR(255)"),
        ("dosage_range", "VARCHAR(255)"),
        ("timing", "VARCHAR(255)"),
        ("duration_minimum", "VARCHAR(100)"),
        ("loading_protocol", "TEXT"),
        ("safety_profile", "VARCHAR(100)"),
        ("adverse_event_rate", "VARCHAR(50)"),
        ("drug_interactions", "TEXT"),
        ("drug_interaction_severity", "VARCHAR(50)"),
        ("age_considerations", "TEXT"),
        ("lifecycle_phase_female", "TEXT"),
        ("grade_rating", "VARCHAR(50)"),
        ("heterogeneity_i2", "VARCHAR(50)"),
        ("publication_bias", "VARCHAR(50)"),
        ("fda_status", "TEXT"),
        ("clinical_guidelines", "TEXT"),
        ("mechanism_pathway", "TEXT"),
        ("absorption_notes", "TEXT"),
        ("form_recommendations", "TEXT"),
        ("time_to_effect", "VARCHAR(100)"),
        ("washout_period", "VARCHAR(100)"),
        ("stacking_synergies", "TEXT"),
        ("stacking_conflicts", "TEXT"),
        ("os_ready", "BOOLEAN DEFAULT FALSE"),
        ("pmid_primary", "BIGINT"),
        ("deprecated", "BOOLEAN DEFAULT FALSE"),
        ("last_sync_at", "TIMESTAMPTZ"),
        ("sync_version", "VARCHAR(20)"),
    ]
    
    with engine.connect() as conn:
        for col_name, col_type in new_columns:
            try:
                conn.execute(text(f"ALTER TABLE ingredients ADD COLUMN IF NOT EXISTS {col_name} {col_type}"))
            except Exception as e:
                print(f"   ⚠️  Column {col_name}: {e}")
        
        # Create index on os_ready for fast filtering
        try:
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_ingredients_os_ready ON ingredients(os_ready)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_ingredients_tier ON ingredients(tier_validated)"))
        except Exception:
            pass
        
        conn.commit()
    
    print("   ✅ Schema extended with new columns")


def load_existing_ingredients() -> Dict[str, int]:
    """Load existing ingredients into a name -> id map."""
    ingredient_map = {}
    
    with engine.connect() as conn:
        result = conn.execute(text("SELECT id, name FROM ingredients"))
        for row in result:
            ingredient_map[row.name.lower()] = row.id
    
    return ingredient_map


def sync_ingredient(conn, row: Dict, existing_map: Dict[str, int], index: int) -> Tuple[str, int, str]:
    """
    Sync a single ingredient row.
    Returns: (action, ingredient_id, name)
    action: 'inserted', 'updated', 'skipped'
    """
    name = safe_str(row.get('ingredient'))
    if not name:
        return ('skipped', 0, 'empty_name')
    
    # Validate critical fields
    tier_validated = normalize_tier(row.get('tier_validated'))
    os_ready = safe_bool(row.get('os_ready'))
    pmid = validate_pmid(row.get('pmid_primary'))
    
    # Prepare biomarker data
    biomarkers_primary = safe_str(row.get('biomarkers_primary'))
    biomarkers_secondary = safe_str(row.get('biomarkers_secondary'))
    
    # Build data dict
    data = {
        'name': name,
        'category': safe_str(row.get('category')),
        'tier_validated': tier_validated,
        'validation_status': safe_str(row.get('validation_status')),
        'validation_notes': safe_str(row.get('validation_notes')),
        'num_meta_analyses': safe_int(row.get('meta_analyses')),
        'num_rcts': safe_int(row.get('rcts')),
        'total_participants': safe_int(row.get('total_participants')),
        'effect_primary': safe_str(row.get('effect_primary')),
        'effect_size_value': safe_float(row.get('effect_size')),
        'effect_ci_lower': safe_float(row.get('effect_ci_lower')),
        'effect_ci_upper': safe_float(row.get('effect_ci_upper')),
        'secondary_effects': safe_str(row.get('secondary_effects')),
        'primary_biomarkers': biomarkers_primary,
        'secondary_biomarkers': biomarkers_secondary,
        'clinical_measures': safe_str(row.get('clinical_measures')),
        'dosage_optimal': safe_str(row.get('dosage_optimal')),
        'dosage_range': safe_str(row.get('dosage_range')),
        'timing': safe_str(row.get('timing')),
        'duration_minimum': safe_str(row.get('duration_minimum')),
        'loading_protocol': safe_str(row.get('loading_protocol')),
        'safety_profile': safe_str(row.get('safety_profile')),
        'adverse_event_rate': safe_str(row.get('adverse_event_rate')),
        'contraindications': safe_str(row.get('contraindications')),
        'drug_interactions': safe_str(row.get('drug_interactions')),
        'drug_interaction_severity': safe_str(row.get('drug_interaction_severity')),
        'male_specific_notes': safe_str(row.get('gender_notes_male')),
        'female_specific_notes': safe_str(row.get('gender_notes_female')),
        'age_considerations': safe_str(row.get('age_considerations')),
        'lifecycle_phase_female': safe_str(row.get('lifecycle_phase_female')),
        'grade_rating': safe_str(row.get('grade_rating')),
        'heterogeneity_i2': safe_str(row.get('heterogeneity_i2')),
        'publication_bias': safe_str(row.get('publication_bias')),
        'fda_status': safe_str(row.get('fda_status')),
        'clinical_guidelines': safe_str(row.get('clinical_guidelines')),
        'mechanism_pathway': safe_str(row.get('mechanism_pathway')),
        'absorption_notes': safe_str(row.get('absorption_notes')),
        'form_recommendations': safe_str(row.get('form_recommendations')),
        'time_to_effect': safe_str(row.get('time_to_effect')),
        'washout_period': safe_str(row.get('washout_period')),
        'stacking_synergies': safe_str(row.get('stacking_synergies')),
        'stacking_conflicts': safe_str(row.get('stacking_conflicts')),
        'os_ready': os_ready,
        'pmid_primary': pmid,
        'deprecated': False,
        'last_sync_at': datetime.utcnow(),
        'sync_version': '2025-12-24',
    }
    
    # Check if ingredient exists
    existing_id = existing_map.get(name.lower())
    
    if existing_id:
        # UPDATE existing ingredient
        update_cols = ', '.join([f"{k} = :{k}" for k in data.keys()])
        data['id'] = existing_id
        conn.execute(text(f"UPDATE ingredients SET {update_cols} WHERE id = :id"), data)
        return ('updated', existing_id, name)
    else:
        # INSERT new ingredient
        sku = generate_sku(data['category'], name, index)
        data['internal_sku'] = sku
        
        columns = ', '.join(data.keys())
        placeholders = ', '.join([f":{k}" for k in data.keys()])
        
        result = conn.execute(text(f"""
            INSERT INTO ingredients ({columns})
            VALUES ({placeholders})
            RETURNING id
        """), data)
        
        new_id = result.fetchone()[0]
        existing_map[name.lower()] = new_id
        return ('inserted', new_id, name)


def sync_synergies(conn, ingredient_id: int, synergies_str: str, conflicts_str: str):
    """Sync stacking synergies and conflicts for an ingredient."""
    if not synergies_str and not conflicts_str:
        return
    
    # Clear existing synergies for this ingredient
    conn.execute(text("""
        DELETE FROM ingredient_synergies WHERE ingredient_id = :ing_id
    """), {'ing_id': ingredient_id})
    
    # Parse and insert synergies
    if synergies_str:
        for item in synergies_str.split(';'):
            item = item.strip()
            if item:
                conn.execute(text("""
                    INSERT INTO ingredient_synergies (ingredient_id, related_ingredient, relationship_type)
                    VALUES (:ing_id, :related, 'Synergistic')
                """), {'ing_id': ingredient_id, 'related': item})
    
    # Parse and insert conflicts
    if conflicts_str:
        for item in conflicts_str.split(';'):
            item = item.strip()
            if item:
                conn.execute(text("""
                    INSERT INTO ingredient_synergies (ingredient_id, related_ingredient, relationship_type)
                    VALUES (:ing_id, :related, 'Antagonistic')
                """), {'ing_id': ingredient_id, 'related': item})


def sync_drug_interactions(conn, ingredient_id: int, interactions_str: str, severity: str):
    """Sync drug interactions for an ingredient."""
    if not interactions_str:
        return
    
    # Clear existing drug interaction risks
    conn.execute(text("""
        DELETE FROM ingredient_risks WHERE ingredient_id = :ing_id AND risk_type = 'Medication'
    """), {'ing_id': ingredient_id})
    
    # Parse and insert interactions
    for drug in interactions_str.split(';'):
        drug = drug.strip()
        if drug:
            conn.execute(text("""
                INSERT INTO ingredient_risks (ingredient_id, risk_type, keyword, severity)
                VALUES (:ing_id, 'Medication', :drug, :severity)
            """), {'ing_id': ingredient_id, 'drug': drug, 'severity': severity or 'Caution'})


def validate_data(df: pd.DataFrame) -> Dict[str, Any]:
    """Validate data before sync. Returns validation report."""
    report = {
        'total_rows': len(df),
        'valid_rows': 0,
        'issues': [],
        'warnings': [],
        'tier_distribution': {},
        'os_ready_count': 0,
        'biomarker_coverage': {'valid': 0, 'unknown': []},
        'missing_required': [],
    }
    
    required_fields = ['ingredient', 'tier_validated']
    
    for idx, row in df.iterrows():
        # Check required fields
        for field in required_fields:
            if pd.isna(row.get(field)) or str(row.get(field)).strip() == '':
                report['missing_required'].append({
                    'row': idx + 2,  # Excel row number
                    'field': field,
                    'ingredient': row.get('ingredient', 'UNKNOWN')
                })
        
        # Validate PMID
        pmid_val = row.get('pmid_primary')
        if pmid_val and not pd.isna(pmid_val):
            pmid = validate_pmid(pmid_val)
            if not pmid:
                report['warnings'].append({
                    'row': idx + 2,
                    'field': 'pmid_primary',
                    'value': pmid_val,
                    'message': 'Invalid PMID format'
                })
        
        # Track tier distribution
        tier = normalize_tier(row.get('tier_validated'))
        report['tier_distribution'][tier] = report['tier_distribution'].get(tier, 0) + 1
        
        # Count OS ready
        if safe_bool(row.get('os_ready')):
            report['os_ready_count'] += 1
        
        # Validate biomarkers
        biomarkers = safe_str(row.get('biomarkers_primary'))
        if biomarkers:
            valid, unknown = validate_biomarkers(biomarkers)
            if unknown:
                report['biomarker_coverage']['unknown'].extend([{
                    'ingredient': row.get('ingredient'),
                    'unknown_biomarker': u
                } for u in unknown])
        
        report['valid_rows'] += 1
    
    return report


def mark_deprecated_ingredients(conn, synced_names: set):
    """Mark ingredients not in the sync set as deprecated."""
    # Get all current ingredient names
    result = conn.execute(text("SELECT id, name FROM ingredients WHERE deprecated IS NOT TRUE"))
    
    deprecated_count = 0
    for row in result:
        if row.name.lower() not in synced_names:
            conn.execute(text("""
                UPDATE ingredients SET deprecated = TRUE, last_sync_at = NOW()
                WHERE id = :id
            """), {'id': row.id})
            deprecated_count += 1
    
    return deprecated_count


def run_sync():
    """Main sync function."""
    print("=" * 70)
    print("GENOMAX² INGREDIENT DATABASE SYNC")
    print(f"Source: {os.path.basename(CSV_FILE)}")
    print(f"Timestamp: {datetime.utcnow().isoformat()}")
    print("=" * 70)
    
    # Check file exists
    if not os.path.exists(CSV_FILE):
        print(f"\n❌ ERROR: CSV file not found: {CSV_FILE}")
        return False
    
    # Load CSV
    print(f"\n📂 Loading CSV...")
    df = pd.read_csv(CSV_FILE)
    print(f"   Found {len(df)} ingredients")
    
    # Validate data
    print("\n🔍 Validating data...")
    validation = validate_data(df)
    
    print(f"   Total rows: {validation['total_rows']}")
    print(f"   Tier distribution:")
    for tier, count in validation['tier_distribution'].items():
        print(f"      - {tier}: {count}")
    print(f"   OS Ready: {validation['os_ready_count']}")
    
    if validation['missing_required']:
        print(f"\n   ⚠️  Missing required fields: {len(validation['missing_required'])}")
        for item in validation['missing_required'][:5]:
            print(f"      Row {item['row']}: {item['ingredient']} - missing {item['field']}")
    
    if validation['warnings']:
        print(f"\n   ⚠️  Warnings: {len(validation['warnings'])}")
        for item in validation['warnings'][:5]:
            print(f"      Row {item['row']}: {item['message']}")
    
    # Extend schema
    extend_schema()
    
    # Load existing ingredients
    print("\n📊 Loading existing ingredients...")
    existing_map = load_existing_ingredients()
    print(f"   Found {len(existing_map)} existing ingredients")
    
    # Perform sync
    print("\n🔄 Syncing ingredients...")
    
    stats = {
        'inserted': 0,
        'updated': 0,
        'skipped': 0,
        'errors': [],
        'synced_names': set()
    }
    
    with engine.connect() as conn:
        for idx, row in df.iterrows():
            try:
                action, ing_id, name = sync_ingredient(conn, row.to_dict(), existing_map, idx)
                stats[action] += 1
                
                if action != 'skipped' and ing_id:
                    stats['synced_names'].add(name.lower())
                    
                    # Sync synergies
                    sync_synergies(
                        conn, ing_id,
                        safe_str(row.get('stacking_synergies')),
                        safe_str(row.get('stacking_conflicts'))
                    )
                    
                    # Sync drug interactions
                    sync_drug_interactions(
                        conn, ing_id,
                        safe_str(row.get('drug_interactions')),
                        safe_str(row.get('drug_interaction_severity'))
                    )
                
                if (idx + 1) % 25 == 0:
                    print(f"   Processed {idx + 1}/{len(df)}...")
                    
            except Exception as e:
                stats['errors'].append({
                    'row': idx + 2,
                    'ingredient': row.get('ingredient', 'UNKNOWN'),
                    'error': str(e)
                })
        
        # Mark deprecated ingredients
        print("\n🗑️  Marking deprecated ingredients...")
        deprecated_count = mark_deprecated_ingredients(conn, stats['synced_names'])
        
        conn.commit()
    
    # Print summary
    print("\n" + "=" * 70)
    print("📈 SYNC SUMMARY")
    print("=" * 70)
    print(f"   Inserted: {stats['inserted']}")
    print(f"   Updated:  {stats['updated']}")
    print(f"   Skipped:  {stats['skipped']}")
    print(f"   Deprecated: {deprecated_count}")
    
    if stats['errors']:
        print(f"\n   ❌ Errors: {len(stats['errors'])}")
        for err in stats['errors'][:5]:
            print(f"      Row {err['row']}: {err['ingredient']} - {err['error']}")
    
    # Verify final state
    print("\n🔍 Verifying final state...")
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN os_ready = TRUE THEN 1 ELSE 0 END) as os_ready,
                SUM(CASE WHEN tier_validated = 'TIER_1_OS_CORE' THEN 1 ELSE 0 END) as tier1,
                SUM(CASE WHEN tier_validated = 'TIER_2_CONTEXTUAL' THEN 1 ELSE 0 END) as tier2,
                SUM(CASE WHEN tier_validated = 'TIER_3_EXPLORATORY' THEN 1 ELSE 0 END) as tier3,
                SUM(CASE WHEN deprecated = TRUE THEN 1 ELSE 0 END) as deprecated
            FROM ingredients
        """)).fetchone()
        
        print(f"   Total ingredients: {result.total}")
        print(f"   OS Ready: {result.os_ready}")
        print(f"   TIER 1 (OS-CORE): {result.tier1}")
        print(f"   TIER 2 (CONTEXTUAL): {result.tier2}")
        print(f"   TIER 3 (EXPLORATORY): {result.tier3}")
        print(f"   Deprecated: {result.deprecated}")
    
    print("\n✅ Sync complete!")
    return True


if __name__ == "__main__":
    success = run_sync()
    exit(0 if success else 1)
