#!/usr/bin/env python3
"""
GenoMAX2 Database Loader - Local Version
Loads CSV data into your local PostgreSQL
"""

import os
import csv
import pandas as pd
from sqlalchemy import create_engine, text

# Your local PostgreSQL connection
DB_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:1!Qaz2wsx@localhost:5432/genomax2')
engine = create_engine(DB_URL)

# Get the directory where this script is located
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

def safe_int(val):
    if pd.isna(val): return None
    try: return int(float(val))
    except: return None

def safe_float(val):
    if pd.isna(val): return None
    try: return float(val)
    except: return None

def safe_str(val, max_len=None):
    if pd.isna(val): return None
    s = str(val).strip()
    if not s or s.lower() == 'nan': return None
    return s[:max_len] if max_len else s

def generate_sku(category, name, index):
    import re
    cat = re.sub(r'[^A-Z]', '', str(category).upper()[:3]) or 'GEN'
    nm = re.sub(r'[^A-Z]', '', str(name).upper()[:3]) or 'UNK'
    return f"GENO-{cat}-{nm}-{index:03d}"


def create_schema():
    """Create database tables"""
    print("\n📊 Creating database schema...")
    
    with engine.connect() as conn:
        conn.execute(text("""
            DROP TABLE IF EXISTS ingredient_synergies CASCADE;
            DROP TABLE IF EXISTS ingredient_modules CASCADE;
            DROP TABLE IF EXISTS fulfillment_catalog CASCADE;
            DROP TABLE IF EXISTS ingredient_risks CASCADE;
            DROP TABLE IF EXISTS ingredient_logic CASCADE;
            DROP TABLE IF EXISTS os_modules CASCADE;
            DROP TABLE IF EXISTS goals CASCADE;
            DROP TABLE IF EXISTS ingredients CASCADE;
        """))
        
        conn.execute(text("""
            CREATE TABLE ingredients (
                id SERIAL PRIMARY KEY,
                internal_sku VARCHAR(50) UNIQUE NOT NULL,
                name VARCHAR(255) NOT NULL,
                category VARCHAR(100),
                subcategory VARCHAR(100),
                evidence_grade CHAR(1),
                evidence_strength_score DECIMAL(3,2),
                num_meta_analyses INT,
                num_rcts INT,
                total_participants INT,
                study_year_start INT,
                study_year_end INT,
                evidence_confidence VARCHAR(50),
                primary_biomarkers TEXT,
                secondary_biomarkers TEXT,
                biomarker_direction VARCHAR(50),
                mechanisms TEXT,
                primary_outcomes TEXT,
                secondary_outcomes TEXT,
                effect_sizes TEXT,
                gender_relevance VARCHAR(20),
                male_relevance_score DECIMAL(3,2),
                female_relevance_score DECIMAL(3,2),
                male_specific_notes TEXT,
                female_specific_notes TEXT,
                timing_instruction VARCHAR(255),
                food_relation VARCHAR(100),
                timing_reason TEXT,
                studied_doses VARCHAR(255),
                practical_dose VARCHAR(255),
                upper_safe_limit VARCHAR(100),
                contraindications TEXT,
                side_effects_common TEXT,
                side_effects_rare TEXT,
                caution_populations TEXT,
                study_references TEXT,
                study_design TEXT,
                study_notes TEXT,
                clinical_notes TEXT,
                reference_pmids TEXT,
                reference_dois TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            );
            
            CREATE TABLE goals (id SERIAL PRIMARY KEY, name VARCHAR(100) UNIQUE NOT NULL);
            CREATE TABLE os_modules (id SERIAL PRIMARY KEY, name VARCHAR(100) UNIQUE NOT NULL);
            
            CREATE TABLE ingredient_logic (
                id SERIAL PRIMARY KEY,
                ingredient_id INT REFERENCES ingredients(id),
                goal_id INT REFERENCES goals(id),
                priority INT DEFAULT 1,
                UNIQUE(ingredient_id, goal_id)
            );
            
            CREATE TABLE ingredient_modules (
                id SERIAL PRIMARY KEY,
                ingredient_id INT REFERENCES ingredients(id),
                module_id INT REFERENCES os_modules(id),
                priority INT DEFAULT 1,
                UNIQUE(ingredient_id, module_id)
            );
            
            CREATE TABLE ingredient_risks (
                id SERIAL PRIMARY KEY,
                ingredient_id INT REFERENCES ingredients(id),
                risk_type VARCHAR(50),
                keyword VARCHAR(255),
                severity VARCHAR(50),
                notes TEXT
            );
            
            CREATE TABLE ingredient_synergies (
                id SERIAL PRIMARY KEY,
                ingredient_id INT REFERENCES ingredients(id),
                related_ingredient VARCHAR(255),
                relationship_type VARCHAR(50),
                condition VARCHAR(100)
            );
            
            CREATE TABLE fulfillment_catalog (
                id SERIAL PRIMARY KEY,
                ingredient_id INT REFERENCES ingredients(id),
                supplier_name VARCHAR(100),
                product_name VARCHAR(255),
                product_url TEXT,
                short_description TEXT,
                serving_info VARCHAR(255),
                base_price DECIMAL(10,2),
                currency VARCHAR(10),
                available_shopify BOOLEAN DEFAULT FALSE,
                available_amazon BOOLEAN DEFAULT FALSE,
                available_tiktok BOOLEAN DEFAULT FALSE,
                notes TEXT,
                is_active BOOLEAN DEFAULT TRUE
            );
            
            CREATE INDEX idx_ingredients_name ON ingredients(name);
            CREATE INDEX idx_ingredients_category ON ingredients(category);
            CREATE INDEX idx_fulfillment_ingredient ON fulfillment_catalog(ingredient_id);
        """))
        conn.commit()
    
    print("   ✅ Schema created")


def load_base_ingredients(filepath):
    """Load GENOMAX_FINAL_140.csv"""
    print(f"\n📊 Loading base ingredients from {os.path.basename(filepath)}...")
    df = pd.read_csv(filepath)
    print(f"   Found {len(df)} ingredients")
    
    ingredient_map = {}
    
    with engine.connect() as conn:
        for idx, row in df.iterrows():
            name = safe_str(row.get('ingredient_name'))
            if not name: continue
            
            sku = generate_sku(row.get('category'), name, idx)
            
            result = conn.execute(text("""
                INSERT INTO ingredients (
                    internal_sku, name, category, subcategory,
                    evidence_grade, evidence_strength_score, num_meta_analyses, num_rcts,
                    total_participants, study_year_start, study_year_end, evidence_confidence,
                    primary_biomarkers, secondary_biomarkers, mechanisms,
                    primary_outcomes, secondary_outcomes, effect_sizes,
                    gender_relevance, male_relevance_score, female_relevance_score,
                    male_specific_notes, female_specific_notes,
                    timing_instruction, food_relation, timing_reason,
                    studied_doses, practical_dose, upper_safe_limit,
                    contraindications, side_effects_common, side_effects_rare, caution_populations,
                    reference_pmids, reference_dois
                ) VALUES (
                    :sku, :name, :category, :subcategory,
                    :evidence_grade, :evidence_strength_score, :num_meta, :num_rcts,
                    :total_participants, :year_start, :year_end, :confidence,
                    :primary_bio, :secondary_bio, :mechanisms,
                    :primary_out, :secondary_out, :effect_sizes,
                    :gender, :male_score, :female_score,
                    :male_notes, :female_notes,
                    :timing, :food, :timing_reason,
                    :studied_doses, :practical_dose, :upper_limit,
                    :contraindications, :side_common, :side_rare, :caution,
                    :pmids, :dois
                ) RETURNING id
            """), {
                'sku': sku, 'name': name,
                'category': safe_str(row.get('category')),
                'subcategory': safe_str(row.get('subcategory')),
                'evidence_grade': safe_str(row.get('evidence_grade'), 1),
                'evidence_strength_score': safe_float(row.get('evidence_strength_score')),
                'num_meta': safe_int(row.get('number_of_meta_analyses')),
                'num_rcts': safe_int(row.get('number_of_rcts')),
                'total_participants': safe_int(row.get('total_participants')),
                'year_start': safe_int(row.get('study_year_range_start')),
                'year_end': safe_int(row.get('study_year_range_end')),
                'confidence': safe_str(row.get('evidence_confidence_level')),
                'primary_bio': safe_str(row.get('primary_biomarkers')),
                'secondary_bio': safe_str(row.get('secondary_biomarkers')),
                'mechanisms': safe_str(row.get('mechanisms_of_action')),
                'primary_out': safe_str(row.get('primary_outcomes')),
                'secondary_out': safe_str(row.get('secondary_outcomes')),
                'effect_sizes': safe_str(row.get('effect_sizes')),
                'gender': 'Both',
                'male_score': safe_float(row.get('male_relevance_score')),
                'female_score': safe_float(row.get('female_relevance_score')),
                'male_notes': safe_str(row.get('male_specific_notes')),
                'female_notes': safe_str(row.get('female_specific_notes')),
                'timing': safe_str(row.get('best_time_of_day')),
                'food': safe_str(row.get('food_relation')),
                'timing_reason': safe_str(row.get('timing_reason')),
                'studied_doses': safe_str(row.get('studied_doses')),
                'practical_dose': safe_str(row.get('practical_consumer_doses')),
                'upper_limit': safe_str(row.get('upper_safe_limit')),
                'contraindications': safe_str(row.get('contraindications')),
                'side_common': safe_str(row.get('side_effects_common')),
                'side_rare': safe_str(row.get('side_effects_rare')),
                'caution': safe_str(row.get('caution_populations')),
                'pmids': safe_str(row.get('reference_pmids')),
                'dois': safe_str(row.get('reference_dois'))
            })
            
            ing_id = result.fetchone()[0]
            ingredient_map[name.lower()] = ing_id
            
            meds = safe_str(row.get('medication_interactions'))
            if meds:
                for med in meds.split(','):
                    med = med.strip()
                    if med:
                        conn.execute(text("""
                            INSERT INTO ingredient_risks (ingredient_id, risk_type, keyword, severity)
                            VALUES (:ing_id, 'Medication', :med, 'Caution')
                        """), {'ing_id': ing_id, 'med': med})
        
        conn.commit()
    
    print(f"   ✅ Loaded {len(ingredient_map)} ingredients")
    return ingredient_map


def load_engine_data(filepath, ingredient_map):
    """Load GENOMAX_OS_Engine_Data.csv"""
    import re
    print(f"\n🧠 Loading engine data from {os.path.basename(filepath)}...")
    
    # Fix CSV parsing issues
    EXPECTED_COLS = 24
    fixed_rows = []
    
    with open(filepath, 'r', encoding='utf-8-sig') as f:
        content = f.read().replace('\r\n', '\n').replace('\r', '\n')
    
    for i, line in enumerate(content.strip().split('\n')):
        reader = csv.reader([line])
        row = next(reader)
        if i == 0:
            fixed_rows.append(row)
            continue
        while len(row) > EXPECTED_COLS:
            row[7] = row[7] + ', ' + row[8]
            row.pop(8)
        while len(row) < EXPECTED_COLS:
            row.append('')
        fixed_rows.append(row)
    
    df = pd.DataFrame(fixed_rows[1:], columns=fixed_rows[0])
    goals_added, modules_added = set(), set()
    
    with engine.connect() as conn:
        for idx, row in enumerate(df.to_dict('records')):
            name = safe_str(row.get('Ingredient_Name'))
            if not name: continue
            
            ing_id = ingredient_map.get(name.lower())
            
            if not ing_id:
                sku = generate_sku(row.get('Category'), name, 200 + idx)
                result = conn.execute(text("""
                    INSERT INTO ingredients (
                        internal_sku, name, category, evidence_grade,
                        primary_biomarkers, biomarker_direction, gender_relevance,
                        timing_instruction, contraindications,
                        study_references, study_design, study_notes, clinical_notes
                    ) VALUES (
                        :sku, :name, :category, :grade,
                        :biomarkers, :direction, :gender,
                        :timing, :contraindications,
                        :refs, :design, :notes, :clinical
                    ) RETURNING id
                """), {
                    'sku': sku, 'name': name,
                    'category': safe_str(row.get('Category')),
                    'grade': safe_str(row.get('Evidence_Level'), 1),
                    'biomarkers': safe_str(row.get('Biomarker_Targets')),
                    'direction': safe_str(row.get('Direction')),
                    'gender': safe_str(row.get('Gender_Relevance')),
                    'timing': safe_str(row.get('Timing_Recommendation')),
                    'contraindications': safe_str(row.get('Contraindicated_For')),
                    'refs': safe_str(row.get('Study_References')),
                    'design': safe_str(row.get('Study_Design')),
                    'notes': safe_str(row.get('Study_Notes')),
                    'clinical': safe_str(row.get('Clinical_Case_Notes'))
                })
                ing_id = result.fetchone()[0]
                ingredient_map[name.lower()] = ing_id
            else:
                conn.execute(text("""
                    UPDATE ingredients SET
                        biomarker_direction = COALESCE(:direction, biomarker_direction),
                        study_references = COALESCE(:refs, study_references),
                        study_design = COALESCE(:design, study_design),
                        study_notes = COALESCE(:notes, study_notes),
                        clinical_notes = COALESCE(:clinical, clinical_notes)
                    WHERE id = :ing_id
                """), {
                    'ing_id': ing_id,
                    'direction': safe_str(row.get('Direction')),
                    'refs': safe_str(row.get('Study_References')),
                    'design': safe_str(row.get('Study_Design')),
                    'notes': safe_str(row.get('Study_Notes')),
                    'clinical': safe_str(row.get('Clinical_Case_Notes'))
                })
            
            # Goals
            for i, goal_col in enumerate(['Goal_1', 'Goal_2', 'Goal_3'], 1):
                goal = safe_str(row.get(goal_col))
                if goal:
                    conn.execute(text("INSERT INTO goals (name) VALUES (:name) ON CONFLICT DO NOTHING"), {'name': goal})
                    goals_added.add(goal)
                    goal_result = conn.execute(text("SELECT id FROM goals WHERE name = :name"), {'name': goal})
                    goal_id = goal_result.fetchone()[0]
                    conn.execute(text("""
                        INSERT INTO ingredient_logic (ingredient_id, goal_id, priority)
                        VALUES (:ing_id, :goal_id, :priority) ON CONFLICT DO NOTHING
                    """), {'ing_id': ing_id, 'goal_id': goal_id, 'priority': i})
            
            # OS Modules
            for i, mod_col in enumerate(['OS_Module_1', 'OS_Module_2', 'OS_Module_3'], 1):
                module = safe_str(row.get(mod_col))
                if module:
                    conn.execute(text("INSERT INTO os_modules (name) VALUES (:name) ON CONFLICT DO NOTHING"), {'name': module})
                    modules_added.add(module)
                    mod_result = conn.execute(text("SELECT id FROM os_modules WHERE name = :name"), {'name': module})
                    mod_id = mod_result.fetchone()[0]
                    conn.execute(text("""
                        INSERT INTO ingredient_modules (ingredient_id, module_id, priority)
                        VALUES (:ing_id, :mod_id, :priority) ON CONFLICT DO NOTHING
                    """), {'ing_id': ing_id, 'mod_id': mod_id, 'priority': i})
            
            # Drug Interactions
            interactions = safe_str(row.get('Drug_Interactions'))
            if interactions:
                for drug in interactions.split(','):
                    drug = drug.strip()
                    if drug:
                        conn.execute(text("""
                            INSERT INTO ingredient_risks (ingredient_id, risk_type, keyword, severity)
                            VALUES (:ing_id, 'Medication', :drug, 'Caution')
                        """), {'ing_id': ing_id, 'drug': drug})
            
            # Synergies
            for rel_type, col in [('Synergistic', 'Synergistic_With'), ('Antagonistic', 'Antagonistic_With'), ('Contraindicated', 'Contraindicated_With')]:
                relations = safe_str(row.get(col))
                if relations:
                    matches = re.findall(r'\[([^\]]+)\]', relations)
                    for match in matches:
                        parts = match.split('(')
                        related = parts[0].strip()
                        condition = parts[1].rstrip(')').strip() if len(parts) > 1 else None
                        conn.execute(text("""
                            INSERT INTO ingredient_synergies (ingredient_id, related_ingredient, relationship_type, condition)
                            VALUES (:ing_id, :related, :rel_type, :condition)
                        """), {'ing_id': ing_id, 'related': related, 'rel_type': rel_type, 'condition': condition})
            
            # Supliful Product
            url = safe_str(row.get('Product_URL_Supliful'))
            product_name = safe_str(row.get('Product_Name_Supliful'))
            if url:
                conn.execute(text("""
                    INSERT INTO fulfillment_catalog (ingredient_id, supplier_name, product_name, product_url, is_active)
                    VALUES (:ing_id, 'Supliful', :product_name, :url, TRUE) ON CONFLICT DO NOTHING
                """), {'ing_id': ing_id, 'product_name': product_name, 'url': url})
        
        conn.commit()
    
    print(f"   ✅ Processed {len(df)} engine records")
    print(f"   ✅ {len(goals_added)} goals, {len(modules_added)} OS modules")
    return ingredient_map


def load_supliful_catalog(filepath, ingredient_map):
    """Load Supliful_GenoMAX_catalog.csv"""
    print(f"\n🛒 Loading Supliful catalog from {os.path.basename(filepath)}...")
    df = pd.read_csv(filepath)
    
    linked, unlinked = 0, 0
    
    with engine.connect() as conn:
        for idx, row in df.iterrows():
            product_name = safe_str(row.get('ProductName'))
            url = safe_str(row.get('ProductURL'))
            if not product_name or not url: continue
            
            ing_id = None
            product_lower = product_name.lower()
            for ing_name, iid in ingredient_map.items():
                if ing_name in product_lower or product_lower in ing_name:
                    ing_id = iid
                    linked += 1
                    break
            if not ing_id:
                unlinked += 1
            
            existing = conn.execute(text("SELECT id FROM fulfillment_catalog WHERE product_url = :url"), {'url': url}).fetchone()
            
            if not existing:
                conn.execute(text("""
                    INSERT INTO fulfillment_catalog (
                        ingredient_id, supplier_name, product_name, product_url,
                        short_description, serving_info, base_price, currency,
                        available_shopify, available_amazon, available_tiktok, notes
                    ) VALUES (
                        :ing_id, 'Supliful', :name, :url,
                        :desc, :serving, :price, :currency,
                        :shopify, :amazon, :tiktok, :notes
                    )
                """), {
                    'ing_id': ing_id, 'name': product_name, 'url': url,
                    'desc': safe_str(row.get('ShortDescription')),
                    'serving': safe_str(row.get('ServingInfo')),
                    'price': safe_float(row.get('BasePrice')),
                    'currency': safe_str(row.get('Currency')) or 'USD',
                    'shopify': bool(safe_int(row.get('Shopify'))),
                    'amazon': bool(safe_int(row.get('Amazon'))),
                    'tiktok': bool(safe_int(row.get('TikTok'))),
                    'notes': safe_str(row.get('Notes'))
                })
        
        conn.commit()
    
    print(f"   ✅ Loaded {len(df)} products ({linked} linked, {unlinked} standalone)")


def print_summary():
    print("\n" + "=" * 60)
    print("📈 GENOMAX2 DATABASE SUMMARY")
    print("=" * 60)
    
    with engine.connect() as conn:
        stats = {
            'Ingredients': conn.execute(text("SELECT COUNT(*) FROM ingredients")).fetchone()[0],
            'Goals': conn.execute(text("SELECT COUNT(*) FROM goals")).fetchone()[0],
            'OS Modules': conn.execute(text("SELECT COUNT(*) FROM os_modules")).fetchone()[0],
            'Goal Links': conn.execute(text("SELECT COUNT(*) FROM ingredient_logic")).fetchone()[0],
            'Module Links': conn.execute(text("SELECT COUNT(*) FROM ingredient_modules")).fetchone()[0],
            'Risk Entries': conn.execute(text("SELECT COUNT(*) FROM ingredient_risks")).fetchone()[0],
            'Synergy Entries': conn.execute(text("SELECT COUNT(*) FROM ingredient_synergies")).fetchone()[0],
            'Products': conn.execute(text("SELECT COUNT(*) FROM fulfillment_catalog")).fetchone()[0],
        }
        
        for label, count in stats.items():
            print(f"   {label}: {count}")


if __name__ == "__main__":
    print("=" * 60)
    print("GENOMAX2 DATABASE LOADER")
    print("=" * 60)
    
    # File paths - look in same directory as script
    FILE1 = os.path.join(SCRIPT_DIR, 'GENOMAX_FINAL_140.csv')
    FILE2 = os.path.join(SCRIPT_DIR, 'GENOMAX_OS_Engine_Data.csv')
    FILE3 = os.path.join(SCRIPT_DIR, 'Supliful_GenoMAX_catalog.csv')
    
    # Check files exist
    missing = []
    for f in [FILE1, FILE2, FILE3]:
        if not os.path.exists(f):
            missing.append(os.path.basename(f))
    
    if missing:
        print(f"\n❌ ERROR: Missing files in {SCRIPT_DIR}:")
        for m in missing:
            print(f"   - {m}")
        print("\nPlease copy the CSV files to this folder.")
        exit(1)
    
    # Run loader
    create_schema()
    ingredient_map = load_base_ingredients(FILE1)
    ingredient_map = load_engine_data(FILE2, ingredient_map)
    load_supliful_catalog(FILE3, ingredient_map)
    print_summary()
    
    print("\n✅ Database ready!")
    print("   Run 'python api_server.py' to start the API")
