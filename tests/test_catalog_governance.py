"""
Catalog Governance Tests (Issue #5)

Tests for SKU metadata validation, coverage reporting, and admin endpoints.

Mandatory test coverage:
- Missing ingredient_tags => AUTO_BLOCKED + INSUFFICIENT_METADATA
- Missing category_tags => AUTO_BLOCKED + INSUFFICIENT_METADATA
- Deterministic ordering + hash stability
- Admin endpoint security (401 without key)
- Coverage math correctness

Version: catalog_governance_v1
"""

import pytest
import json
from datetime import datetime
from unittest.mock import patch, MagicMock

# Import models and functions
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from app.catalog.models import (
    CatalogSkuMetaV1,
    SkuValidationStatus,
    SkuValidationResult,
    CatalogValidationRunV1,
    CatalogCoverageReportV1,
    GenderLine,
    ReasonCode,
)
from app.catalog.validate import (
    validate_sku,
    get_valid_skus,
    get_blocked_skus,
    get_unknown_ingredients_summary,
)


class TestSkuValidation:
    """Tests for SKU validation logic."""
    
    def test_valid_sku_passes(self):
        """SKU with all required fields passes validation."""
        meta = CatalogSkuMetaV1(
            sku_id="vitamin-d3-2000-iu",
            product_name="Vitamin D3 2000 IU",
            ingredient_tags=["vitamin_d3"],
            category_tags=["vitamin"],
            risk_tags=[],
            gender_line=GenderLine.UNISEX,
        )
        
        result = validate_sku(meta, [])
        
        assert result.status == SkuValidationStatus.VALID
        assert len(result.reason_codes) == 0
        assert len(result.missing_fields) == 0
        assert result.metadata is not None
    
    def test_missing_ingredient_tags_blocked(self):
        """Missing ingredient_tags results in AUTO_BLOCKED."""
        meta = CatalogSkuMetaV1(
            sku_id="unknown-product",
            product_name="Unknown Product",
            ingredient_tags=[],  # Empty = missing
            category_tags=["specialty"],
            risk_tags=[],
            gender_line=GenderLine.UNISEX,
        )
        
        result = validate_sku(meta, ["Unknown Product"])
        
        assert result.status == SkuValidationStatus.AUTO_BLOCKED
        assert ReasonCode.INSUFFICIENT_METADATA in result.reason_codes
        assert ReasonCode.EMPTY_INGREDIENT_TAGS in result.reason_codes
        assert "ingredient_tags" in result.missing_fields
    
    def test_empty_ingredient_tags_blocked(self):
        """Empty ingredient_tags array results in AUTO_BLOCKED."""
        meta = CatalogSkuMetaV1(
            sku_id="empty-ingredients",
            product_name="Empty Ingredients",
            ingredient_tags=[],
            category_tags=["vitamin"],
            risk_tags=[],
            gender_line=GenderLine.UNISEX,
        )
        
        result = validate_sku(meta, [])
        
        assert result.status == SkuValidationStatus.AUTO_BLOCKED
        assert ReasonCode.EMPTY_INGREDIENT_TAGS in result.reason_codes
    
    def test_missing_category_tags_blocked(self):
        """Missing category_tags results in AUTO_BLOCKED."""
        meta = CatalogSkuMetaV1(
            sku_id="no-category",
            product_name="No Category Product",
            ingredient_tags=["vitamin_d3"],
            category_tags=[],  # Empty = missing
            risk_tags=[],
            gender_line=GenderLine.UNISEX,
        )
        
        result = validate_sku(meta, [])
        
        assert result.status == SkuValidationStatus.AUTO_BLOCKED
        assert ReasonCode.INSUFFICIENT_METADATA in result.reason_codes
        assert ReasonCode.EMPTY_CATEGORY_TAGS in result.reason_codes
        assert "category_tags" in result.missing_fields
    
    def test_blocked_evidence_tier_blocked(self):
        """SKU with BLOCKED evidence tier is AUTO_BLOCKED."""
        meta = CatalogSkuMetaV1(
            sku_id="ashwagandha",
            product_name="Ashwagandha",
            ingredient_tags=["ashwagandha"],
            category_tags=["adaptogen"],
            risk_tags=["blocked_ingredient"],
            gender_line=GenderLine.UNISEX,
            evidence_tier="❌ BLOCKED - DO NOT SELL",
        )
        
        result = validate_sku(meta, [])
        
        assert result.status == SkuValidationStatus.AUTO_BLOCKED
        assert ReasonCode.BLOCKED_BY_EVIDENCE in result.reason_codes
        assert ReasonCode.HEPATOTOXICITY_RISK in result.reason_codes
    
    def test_unknown_ingredients_tracked(self):
        """Unknown ingredients are tracked but don't auto-block if tags exist."""
        meta = CatalogSkuMetaV1(
            sku_id="product-with-unknown",
            product_name="Product With Unknown",
            ingredient_tags=["vitamin_d3"],  # Has valid tag
            category_tags=["vitamin"],
            risk_tags=[],
            gender_line=GenderLine.UNISEX,
        )
        
        # Has unknown ingredients but also has valid tags
        result = validate_sku(meta, ["some unknown ingredient"])
        
        assert result.status == SkuValidationStatus.VALID
        assert "some unknown ingredient" in result.unknown_ingredients


class TestDeterminism:
    """Tests for deterministic output."""
    
    def test_hash_stability(self):
        """Same results produce same hash."""
        results = [
            SkuValidationResult(
                sku_id="product-a",
                product_name="Product A",
                status=SkuValidationStatus.VALID,
                reason_codes=[],
                missing_fields=[],
                unknown_ingredients=[],
            ),
            SkuValidationResult(
                sku_id="product-b",
                product_name="Product B",
                status=SkuValidationStatus.AUTO_BLOCKED,
                reason_codes=["INSUFFICIENT_METADATA"],
                missing_fields=["ingredient_tags"],
                unknown_ingredients=[],
            ),
        ]
        
        hash1 = CatalogValidationRunV1.compute_results_hash(results)
        hash2 = CatalogValidationRunV1.compute_results_hash(results)
        
        assert hash1 == hash2
        assert len(hash1) == 64  # SHA256 hex
    
    def test_hash_changes_with_different_input(self):
        """Different results produce different hashes."""
        results1 = [
            SkuValidationResult(
                sku_id="product-a",
                product_name="Product A",
                status=SkuValidationStatus.VALID,
                reason_codes=[],
                missing_fields=[],
                unknown_ingredients=[],
            ),
        ]
        
        results2 = [
            SkuValidationResult(
                sku_id="product-a",
                product_name="Product A",
                status=SkuValidationStatus.AUTO_BLOCKED,  # Different status
                reason_codes=["INSUFFICIENT_METADATA"],
                missing_fields=["ingredient_tags"],
                unknown_ingredients=[],
            ),
        ]
        
        hash1 = CatalogValidationRunV1.compute_results_hash(results1)
        hash2 = CatalogValidationRunV1.compute_results_hash(results2)
        
        assert hash1 != hash2
    
    def test_deterministic_ordering(self):
        """Results are sorted by sku_id for determinism."""
        results = [
            SkuValidationResult(
                sku_id="zebra-product",
                product_name="Zebra",
                status=SkuValidationStatus.VALID,
                reason_codes=[],
                missing_fields=[],
                unknown_ingredients=[],
            ),
            SkuValidationResult(
                sku_id="alpha-product",
                product_name="Alpha",
                status=SkuValidationStatus.VALID,
                reason_codes=[],
                missing_fields=[],
                unknown_ingredients=[],
            ),
        ]
        
        # Hash should be same regardless of input order
        hash1 = CatalogValidationRunV1.compute_results_hash(results)
        hash2 = CatalogValidationRunV1.compute_results_hash(list(reversed(results)))
        
        assert hash1 == hash2


class TestCoverageReport:
    """Tests for coverage report generation."""
    
    def test_coverage_math_correctness(self):
        """Coverage percentages are calculated correctly."""
        results = [
            SkuValidationResult(
                sku_id=f"product-{i}",
                product_name=f"Product {i}",
                status=SkuValidationStatus.VALID if i < 3 else SkuValidationStatus.AUTO_BLOCKED,
                reason_codes=[] if i < 3 else ["INSUFFICIENT_METADATA"],
                missing_fields=[] if i < 3 else ["ingredient_tags"],
                unknown_ingredients=[],
            )
            for i in range(10)
        ]
        
        coverage = CatalogCoverageReportV1.from_results(results)
        
        assert coverage.total_skus == 10
        assert coverage.valid_skus == 3
        assert coverage.auto_blocked_skus == 7
        assert coverage.percent_valid == 30.0
    
    def test_missing_fields_counted(self):
        """Missing fields are counted correctly."""
        results = [
            SkuValidationResult(
                sku_id="product-1",
                product_name="Product 1",
                status=SkuValidationStatus.AUTO_BLOCKED,
                reason_codes=["INSUFFICIENT_METADATA"],
                missing_fields=["ingredient_tags"],
                unknown_ingredients=[],
            ),
            SkuValidationResult(
                sku_id="product-2",
                product_name="Product 2",
                status=SkuValidationStatus.AUTO_BLOCKED,
                reason_codes=["INSUFFICIENT_METADATA"],
                missing_fields=["ingredient_tags", "category_tags"],
                unknown_ingredients=[],
            ),
        ]
        
        coverage = CatalogCoverageReportV1.from_results(results)
        
        assert coverage.top_missing_fields["ingredient_tags"] == 2
        assert coverage.top_missing_fields["category_tags"] == 1
    
    def test_zero_division_handled(self):
        """Empty results don't cause division by zero."""
        coverage = CatalogCoverageReportV1.from_results([])
        
        assert coverage.total_skus == 0
        assert coverage.percent_valid == 0.0


class TestFilterFunctions:
    """Tests for filter helper functions."""
    
    def test_get_valid_skus(self):
        """get_valid_skus returns only VALID status."""
        results = [
            SkuValidationResult(
                sku_id="valid-1",
                product_name="Valid 1",
                status=SkuValidationStatus.VALID,
                reason_codes=[],
                missing_fields=[],
                unknown_ingredients=[],
            ),
            SkuValidationResult(
                sku_id="blocked-1",
                product_name="Blocked 1",
                status=SkuValidationStatus.AUTO_BLOCKED,
                reason_codes=["INSUFFICIENT_METADATA"],
                missing_fields=["ingredient_tags"],
                unknown_ingredients=[],
            ),
        ]
        
        valid = get_valid_skus(results)
        
        assert len(valid) == 1
        assert valid[0].sku_id == "valid-1"
    
    def test_get_blocked_skus(self):
        """get_blocked_skus returns only AUTO_BLOCKED status."""
        results = [
            SkuValidationResult(
                sku_id="valid-1",
                product_name="Valid 1",
                status=SkuValidationStatus.VALID,
                reason_codes=[],
                missing_fields=[],
                unknown_ingredients=[],
            ),
            SkuValidationResult(
                sku_id="blocked-1",
                product_name="Blocked 1",
                status=SkuValidationStatus.AUTO_BLOCKED,
                reason_codes=["INSUFFICIENT_METADATA"],
                missing_fields=["ingredient_tags"],
                unknown_ingredients=[],
            ),
        ]
        
        blocked = get_blocked_skus(results)
        
        assert len(blocked) == 1
        assert blocked[0].sku_id == "blocked-1"
    
    def test_unknown_ingredients_summary(self):
        """Unknown ingredients are grouped by frequency."""
        results = [
            SkuValidationResult(
                sku_id="product-1",
                product_name="Product 1",
                status=SkuValidationStatus.VALID,
                reason_codes=[],
                missing_fields=[],
                unknown_ingredients=["unknown-a", "unknown-b"],
            ),
            SkuValidationResult(
                sku_id="product-2",
                product_name="Product 2",
                status=SkuValidationStatus.VALID,
                reason_codes=[],
                missing_fields=[],
                unknown_ingredients=["unknown-a"],  # Same as product-1
            ),
        ]
        
        summary = get_unknown_ingredients_summary(results)
        
        assert "unknown-a" in summary
        assert len(summary["unknown-a"]) == 2  # Used by 2 products
        assert len(summary["unknown-b"]) == 1


class TestModelValidation:
    """Tests for Pydantic model validation."""
    
    def test_tag_normalization(self):
        """Tags are normalized to lowercase."""
        meta = CatalogSkuMetaV1(
            sku_id="test",
            product_name="Test",
            ingredient_tags=["VITAMIN_D3", "Omega3"],
            category_tags=["VITAMIN"],
        )
        
        assert "vitamin_d3" in meta.ingredient_tags
        assert "omega3" in meta.ingredient_tags
        assert "vitamin" in meta.category_tags
    
    def test_string_to_list_conversion(self):
        """Comma-separated strings are converted to lists."""
        meta = CatalogSkuMetaV1(
            sku_id="test",
            product_name="Test",
            ingredient_tags="vitamin_d3, omega3, magnesium",
            category_tags="vitamin, mineral",
        )
        
        assert len(meta.ingredient_tags) == 3
        assert "vitamin_d3" in meta.ingredient_tags
        assert "omega3" in meta.ingredient_tags
        assert "magnesium" in meta.ingredient_tags
    
    def test_gender_line_enum(self):
        """Gender line accepts valid enum values."""
        meta = CatalogSkuMetaV1(
            sku_id="test",
            product_name="Test",
            ingredient_tags=["vitamin_d3"],
            category_tags=["vitamin"],
            gender_line=GenderLine.MAXIMO2,
        )
        
        assert meta.gender_line == GenderLine.MAXIMO2


class TestAdminEndpointSecurity:
    """Tests for admin endpoint security."""
    
    def test_missing_api_key_returns_401(self):
        """Missing X-Admin-API-Key header returns 401."""
        from app.catalog.admin import verify_admin_key
        from fastapi import HTTPException
        
        # Set expected key
        with patch.dict(os.environ, {"ADMIN_API_KEY": "test-secret-key"}):
            with pytest.raises(HTTPException) as exc_info:
                verify_admin_key(None)
            
            assert exc_info.value.status_code == 401
            assert "Missing" in exc_info.value.detail
    
    def test_invalid_api_key_returns_401(self):
        """Invalid X-Admin-API-Key returns 401."""
        from app.catalog.admin import verify_admin_key
        from fastapi import HTTPException
        
        with patch.dict(os.environ, {"ADMIN_API_KEY": "correct-key"}):
            with pytest.raises(HTTPException) as exc_info:
                verify_admin_key("wrong-key")
            
            assert exc_info.value.status_code == 401
            assert "Invalid" in exc_info.value.detail
    
    def test_valid_api_key_passes(self):
        """Valid X-Admin-API-Key passes verification."""
        from app.catalog.admin import verify_admin_key
        
        with patch.dict(os.environ, {"ADMIN_API_KEY": "correct-key"}):
            result = verify_admin_key("correct-key")
            assert result == "correct-key"


# Run tests
if __name__ == "__main__":
    pytest.main([__file__, "-v"])
