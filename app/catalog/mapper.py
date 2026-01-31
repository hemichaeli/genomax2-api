"""
Catalog Mapper (Issue #5)

Maps raw catalog data to CatalogSkuMetaV1 with canonical ingredient tags.

Version: catalog_governance_v1.1

CHANGELOG v1.1:
- Updated infer_gender_line() to handle removal of UNISEX
- Products without explicit gender indicators default to MAXIMO2 (requires manual review)
- Added os_environment parameter support for database-driven gender line
"""

import json
import os
import csv
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
import re

from .models import CatalogSkuMetaV1, GenderLine


class IngredientTagDictionary:
    """
    Canonical ingredient tag dictionary for mapping raw names to standardized tags.
    """
    
    def __init__(self, dictionary_path: Optional[str] = None):
        """
        Initialize with dictionary JSON file.
        
        Args:
            dictionary_path: Path to ingredient_tag_dictionary.v1.json
        """
        if dictionary_path is None:
            # Default path relative to repo root
            dictionary_path = os.path.join(
                os.path.dirname(__file__),
                '..', '..', 'config', 'catalog', 'ingredient_tag_dictionary.v1.json'
            )
        
        self.dictionary_path = Path(dictionary_path)
        self._load_dictionary()
    
    def _load_dictionary(self):
        """Load and index the dictionary for fast lookups."""
        if not self.dictionary_path.exists():
            raise FileNotFoundError(f"Dictionary not found: {self.dictionary_path}")
        
        with open(self.dictionary_path, 'r', encoding='utf-8') as f:
            self.raw_dict = json.load(f)
        
        self.canonical_tags = self.raw_dict.get('canonical_tags', {})
        self.category_mappings = self.raw_dict.get('category_mappings', {})
        self.blocked_ingredients = self.raw_dict.get('blocked_ingredients', {})
        
        # Build reverse lookup: alias -> canonical_tag
        self._alias_index: Dict[str, str] = {}
        for tag, data in self.canonical_tags.items():
            for alias in data.get('aliases', []):
                self._alias_index[alias.lower().strip()] = tag
    
    def lookup(self, raw_name: str) -> Optional[str]:
        """
        Look up canonical tag for a raw ingredient name.
        
        Args:
            raw_name: Raw ingredient name (e.g., "Vitamin D3 2000 IU")
            
        Returns:
            Canonical tag (e.g., "vitamin_d3") or None if not found
        """
        if not raw_name:
            return None
        
        normalized = raw_name.lower().strip()
        
        # Direct alias match
        if normalized in self._alias_index:
            return self._alias_index[normalized]
        
        # Try partial match on product names (for complex product names)
        for alias, tag in self._alias_index.items():
            if alias in normalized or normalized in alias:
                return tag
        
        return None
    
    def get_tag_data(self, tag: str) -> Optional[Dict[str, Any]]:
        """Get full data for a canonical tag."""
        return self.canonical_tags.get(tag)
    
    def get_category_tags(self, supliful_category: str) -> List[str]:
        """Map Supliful category to canonical category tags."""
        return self.category_mappings.get(supliful_category, [])
    
    def is_blocked(self, tag: str) -> bool:
        """Check if an ingredient tag is blocked."""
        return tag in self.blocked_ingredients
    
    def get_risk_tags(self, tag: str) -> List[str]:
        """Get risk tags for a canonical ingredient tag."""
        data = self.get_tag_data(tag)
        if data:
            return data.get('risk_tags', [])
        return []


class CatalogMapper:
    """
    Maps raw catalog data to validated CatalogSkuMetaV1 objects.
    """
    
    def __init__(self, dictionary: Optional[IngredientTagDictionary] = None):
        """
        Initialize mapper.
        
        Args:
            dictionary: Ingredient tag dictionary (creates default if None)
        """
        self.dictionary = dictionary or IngredientTagDictionary()
    
    def slugify(self, name: str) -> str:
        """Convert product name to URL-safe slug."""
        slug = name.lower()
        slug = re.sub(r'[^a-z0-9\s-]', '', slug)
        slug = re.sub(r'[\s]+', '-', slug)
        slug = re.sub(r'-+', '-', slug)
        return slug.strip('-')
    
    def os_environment_to_gender_line(self, os_environment: Optional[str]) -> Optional[GenderLine]:
        """
        Convert os_environment value to GenderLine enum.
        
        Args:
            os_environment: Canonical os_environment value ("MAXimo²" or "MAXima²")
            
        Returns:
            GenderLine enum value or None if invalid/missing
        """
        if not os_environment:
            return None
        
        env_normalized = os_environment.lower().strip()
        if env_normalized in ("maximo²", "maximo2"):
            return GenderLine.MAXIMO2
        elif env_normalized in ("maxima²", "maxima2"):
            return GenderLine.MAXIMA2
        
        return None
    
    def infer_gender_line(
        self, 
        product_name: str, 
        category: str,
        os_environment: Optional[str] = None
    ) -> GenderLine:
        """
        Infer gender targeting from product name, category, or os_environment.
        
        Priority:
        1. os_environment (canonical source of truth from database)
        2. Product name indicators ("Men's", "Women's", etc.)
        3. Default to MAXIMO2 (requires manual review for ambiguous products)
        
        Note: Post-migration 016, UNISEX no longer exists. All products must be 
        explicitly MAXimo² or MAXima². Products without clear indicators default
        to MAXIMO2 and should be manually reviewed.
        
        Args:
            product_name: Product display name
            category: Product category
            os_environment: Optional canonical os_environment value from database
            
        Returns:
            GenderLine.MAXIMO2 or GenderLine.MAXIMA2
        """
        # Priority 1: Use os_environment if provided
        if os_environment:
            gender_line = self.os_environment_to_gender_line(os_environment)
            if gender_line:
                return gender_line
        
        # Priority 2: Infer from product name
        name_lower = product_name.lower()
        
        if any(term in name_lower for term in ["men's", "mens", "male", "prostate"]):
            return GenderLine.MAXIMO2
        
        if any(term in name_lower for term in ["women's", "womens", "female", "prenatal"]):
            return GenderLine.MAXIMA2
        
        # Priority 3: Default to MAXIMO2 (no UNISEX post-migration 016)
        # NOTE: Products hitting this default should be manually reviewed
        return GenderLine.MAXIMO2
    
    def map_from_merged_csv_row(
        self, 
        row: Dict[str, Any],
        os_environment: Optional[str] = None
    ) -> Tuple[CatalogSkuMetaV1, List[str]]:
        """
        Map a row from GENOMAX2_SUPLIFUL_CATALOG.csv (merged format).
        
        Args:
            row: CSV row dictionary
            os_environment: Optional canonical os_environment override
        
        Returns:
            Tuple of (CatalogSkuMetaV1, unknown_ingredients)
        """
        product_name = row.get('supliful_sku', '').strip()
        unknown_ingredients = []
        
        # Get matched ingredient from pre-mapped data
        genomax_ingredient = row.get('genomax_ingredient', '').strip()
        
        ingredient_tags = []
        risk_tags = []
        
        if genomax_ingredient:
            tag = self.dictionary.lookup(genomax_ingredient)
            if tag:
                ingredient_tags.append(tag)
                risk_tags.extend(self.dictionary.get_risk_tags(tag))
                
                # Check if blocked
                if self.dictionary.is_blocked(tag):
                    risk_tags.append('blocked_ingredient')
            else:
                unknown_ingredients.append(genomax_ingredient)
        
        # Map category
        category = row.get('category', '').strip()
        category_tags = self.dictionary.get_category_tags(category)
        
        # Evidence tier
        evidence_tier = row.get('evidence_tier', '').strip()
        
        # Sell recommendation
        sell_recommendation = row.get('sell_recommendation', '').strip()
        
        # Contraindications and drug interactions
        contraindications = []
        if row.get('contraindications'):
            contraindications = [c.strip() for c in row['contraindications'].split(';') if c.strip()]
        
        drug_interactions = []
        if row.get('drug_interactions'):
            drug_interactions = [d.strip() for d in row['drug_interactions'].split(';') if d.strip()]
        
        # Use os_environment from row if available, or passed parameter
        row_os_env = row.get('os_environment') or os_environment
        
        meta = CatalogSkuMetaV1(
            sku_id=self.slugify(product_name),
            product_name=product_name,
            product_url=row.get('supliful_url'),
            ingredient_tags=ingredient_tags,
            category_tags=category_tags,
            risk_tags=list(set(risk_tags)),
            gender_line=self.infer_gender_line(product_name, category, row_os_env),
            evidence_tier=evidence_tier if evidence_tier else None,
            sell_recommendation=sell_recommendation if sell_recommendation else None,
            contraindications=contraindications,
            drug_interactions=drug_interactions,
            updated_at=datetime.utcnow(),
        )
        
        return meta, unknown_ingredients
    
    def map_from_full_catalog_row(
        self, 
        row: Dict[str, Any],
        os_environment: Optional[str] = None
    ) -> Tuple[CatalogSkuMetaV1, List[str]]:
        """
        Map a row from Supliful_GenoMAX_catalog.csv (full format without ingredient mapping).
        
        Args:
            row: CSV row dictionary
            os_environment: Optional canonical os_environment override
        
        Returns:
            Tuple of (CatalogSkuMetaV1, unknown_ingredients)
        """
        product_name = row.get('ProductName', '').strip()
        unknown_ingredients = []
        
        # Try to infer ingredient from product name
        ingredient_tags = []
        risk_tags = []
        
        tag = self.dictionary.lookup(product_name)
        if tag:
            ingredient_tags.append(tag)
            risk_tags.extend(self.dictionary.get_risk_tags(tag))
            if self.dictionary.is_blocked(tag):
                risk_tags.append('blocked_ingredient')
        else:
            # Product name itself becomes unknown
            unknown_ingredients.append(product_name)
        
        # Map category
        category = row.get('Category', '').strip()
        category_tags = self.dictionary.get_category_tags(category)
        
        # Use os_environment from row if available, or passed parameter
        row_os_env = row.get('os_environment') or os_environment
        
        meta = CatalogSkuMetaV1(
            sku_id=self.slugify(product_name),
            product_name=product_name,
            product_url=row.get('ProductURL'),
            ingredient_tags=ingredient_tags,
            category_tags=category_tags,
            risk_tags=list(set(risk_tags)),
            gender_line=self.infer_gender_line(product_name, category, row_os_env),
            updated_at=datetime.utcnow(),
        )
        
        return meta, unknown_ingredients
    
    def load_merged_catalog(self, csv_path: str) -> List[Tuple[CatalogSkuMetaV1, List[str]]]:
        """
        Load SKUs from merged catalog CSV.
        
        Args:
            csv_path: Path to GENOMAX2_SUPLIFUL_CATALOG.csv
            
        Returns:
            List of (CatalogSkuMetaV1, unknown_ingredients) tuples
        """
        results = []
        
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                meta, unknown = self.map_from_merged_csv_row(row)
                results.append((meta, unknown))
        
        return results
    
    def load_full_catalog(self, csv_path: str) -> List[Tuple[CatalogSkuMetaV1, List[str]]]:
        """
        Load SKUs from full Supliful catalog CSV.
        
        Args:
            csv_path: Path to Supliful_GenoMAX_catalog.csv
            
        Returns:
            List of (CatalogSkuMetaV1, unknown_ingredients) tuples
        """
        results = []
        
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                meta, unknown = self.map_from_full_catalog_row(row)
                results.append((meta, unknown))
        
        return results
    
    def load_catalog_auto(self) -> List[Tuple[CatalogSkuMetaV1, List[str]]]:
        """
        Auto-detect and load catalog from known paths.
        
        Tries merged catalog first (has ingredient mappings), 
        falls back to full catalog.
        
        Returns:
            List of (CatalogSkuMetaV1, unknown_ingredients) tuples
        """
        repo_root = Path(__file__).parent.parent.parent
        
        # Try merged catalog first (preferred - has ingredient mappings)
        merged_path = repo_root / 'data' / 'GENOMAX2_SUPLIFUL_CATALOG.csv'
        if merged_path.exists():
            return self.load_merged_catalog(str(merged_path))
        
        # Fall back to full catalog
        full_path = repo_root / 'Supliful_GenoMAX_catalog.csv'
        if full_path.exists():
            return self.load_full_catalog(str(full_path))
        
        raise FileNotFoundError(
            f"No catalog found. Tried:\n"
            f"  - {merged_path}\n"
            f"  - {full_path}"
        )
