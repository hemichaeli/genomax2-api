"""
GenoMAX² Catalog Brain Wiring (Issue #15)
Connects recommendation engine to real product inventory.

PRINCIPLE: Only recommend products that can actually be sold.

This module:
1. Loads catalog_products as the canonical SKU universe
2. Provides filter function for routing/matching layers
3. Hard aborts (503) if catalog is unavailable
4. Blocked SKUs never enter the pipeline

CRITICAL: No fallback, no mocks. If catalog fails, entire pipeline fails.

Version: catalog_wiring_v1
"""

import os
import threading
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Any
from dataclasses import dataclass, field
from enum import Enum
import psycopg2
from psycopg2.extras import RealDictCursor


DATABASE_URL = os.getenv("DATABASE_URL")

CATALOG_WIRING_VERSION = "catalog_wiring_v1"


class CatalogStatus(str, Enum):
    """Catalog loading status."""
    NOT_LOADED = "NOT_LOADED"
    LOADING = "LOADING"
    LOADED = "LOADED"
    FAILED = "FAILED"


@dataclass
class CatalogProduct:
    """A purchasable product from Supliful catalog."""
    sku: str
    supliful_id: str
    name: str
    product_line: str  # MAXimo² or MAXima²
    category: str
    active: bool
    price_usd: float
    wholesale_price_usd: float
    ingredient_tags: List[str] = field(default_factory=list)
    governance_status: Optional[str] = None  # ACTIVE, BLOCKED, PENDING
    
    def is_available(self) -> bool:
        """Check if product is available for recommendation."""
        return self.active and self.governance_status != "BLOCKED"


class CatalogWiringError(Exception):
    """Raised when catalog is unavailable. Results in 503 Service Unavailable."""
    pass


class CatalogWiring:
    """
    Singleton that manages the canonical SKU universe.
    
    IMPORTANT: This is the ONLY source of truth for purchasable products.
    The Brain MUST check this before recommending any SKU.
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        self._products: Dict[str, CatalogProduct] = {}
        self._sku_set: Set[str] = set()
        self._maximo_skus: Set[str] = set()
        self._maxima_skus: Set[str] = set()
        self._status = CatalogStatus.NOT_LOADED
        self._loaded_at: Optional[datetime] = None
        self._error: Optional[str] = None
        self._initialized = True
    
    @property
    def status(self) -> CatalogStatus:
        return self._status
    
    @property
    def is_loaded(self) -> bool:
        return self._status == CatalogStatus.LOADED
    
    @property
    def product_count(self) -> int:
        return len(self._products)
    
    @property
    def available_skus(self) -> Set[str]:
        """All SKUs that are available for recommendation."""
        return {sku for sku, p in self._products.items() if p.is_available()}
    
    def load(self, force_reload: bool = False) -> Dict[str, Any]:
        """
        Load catalog from database.
        
        CRITICAL: If this fails, raise CatalogWiringError.
        No fallback, no mocks, no empty defaults.
        
        Args:
            force_reload: If True, reload even if already loaded
            
        Returns:
            Loading statistics
            
        Raises:
            CatalogWiringError: If catalog cannot be loaded
        """
        if self._status == CatalogStatus.LOADED and not force_reload:
            return self._get_stats()
        
        self._status = CatalogStatus.LOADING
        self._error = None
        
        try:
            conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
            cur = conn.cursor()
            
            # Query catalog_products table
            cur.execute("""
                SELECT 
                    sku,
                    supliful_id,
                    name,
                    product_line,
                    category,
                    active,
                    price_usd,
                    wholesale_price_usd,
                    ingredient_tags,
                    governance_status
                FROM catalog_products
                WHERE active = true
                ORDER BY product_line, sku
            """)
            
            rows = cur.fetchall()
            
            if not rows:
                cur.close()
                conn.close()
                self._status = CatalogStatus.FAILED
                self._error = "No active products found in catalog_products table"
                raise CatalogWiringError(
                    "CATALOG_EMPTY: No active products in catalog. "
                    "Cannot proceed with recommendations."
                )
            
            # Load products into memory
            self._products.clear()
            self._sku_set.clear()
            self._maximo_skus.clear()
            self._maxima_skus.clear()
            
            for row in rows:
                product = CatalogProduct(
                    sku=row['sku'],
                    supliful_id=row['supliful_id'] or '',
                    name=row['name'],
                    product_line=row['product_line'] or 'MAXimo²',
                    category=row['category'] or 'supplement',
                    active=row['active'],
                    price_usd=float(row['price_usd'] or 0),
                    wholesale_price_usd=float(row['wholesale_price_usd'] or 0),
                    ingredient_tags=row['ingredient_tags'] or [],
                    governance_status=row.get('governance_status', 'ACTIVE'),
                )
                
                if product.is_available():
                    self._products[product.sku] = product
                    self._sku_set.add(product.sku)
                    
                    # Index by product line
                    if 'maximo' in product.product_line.lower():
                        self._maximo_skus.add(product.sku)
                    elif 'maxima' in product.product_line.lower():
                        self._maxima_skus.add(product.sku)
            
            cur.close()
            conn.close()
            
            self._status = CatalogStatus.LOADED
            self._loaded_at = datetime.now(timezone.utc)
            
            return self._get_stats()
            
        except psycopg2.Error as e:
            self._status = CatalogStatus.FAILED
            self._error = f"Database error: {str(e)}"
            raise CatalogWiringError(
                f"CATALOG_DB_ERROR: Cannot connect to catalog database. "
                f"Error: {str(e)}"
            )
        except Exception as e:
            if isinstance(e, CatalogWiringError):
                raise
            self._status = CatalogStatus.FAILED
            self._error = str(e)
            raise CatalogWiringError(
                f"CATALOG_LOAD_ERROR: Unexpected error loading catalog. "
                f"Error: {str(e)}"
            )
    
    def _get_stats(self) -> Dict[str, Any]:
        """Get catalog statistics."""
        return {
            "status": self._status.value,
            "total_products": len(self._products),
            "maximo_products": len(self._maximo_skus),
            "maxima_products": len(self._maxima_skus),
            "loaded_at": self._loaded_at.isoformat() if self._loaded_at else None,
            "version": CATALOG_WIRING_VERSION,
        }
    
    def ensure_loaded(self) -> None:
        """
        Ensure catalog is loaded. Call this at the start of any pipeline.
        
        Raises:
            CatalogWiringError: If catalog cannot be loaded
        """
        if not self.is_loaded:
            self.load()
    
    def is_purchasable(self, sku: str) -> bool:
        """
        Check if a SKU is purchasable.
        
        MUST be called AFTER ensure_loaded().
        
        Args:
            sku: The SKU to check
            
        Returns:
            True if SKU exists in catalog and is available
        """
        return sku in self._sku_set
    
    def get_product(self, sku: str) -> Optional[CatalogProduct]:
        """
        Get product details by SKU.
        
        Args:
            sku: The SKU to look up
            
        Returns:
            CatalogProduct if found, None otherwise
        """
        return self._products.get(sku)
    
    def filter_skus(self, skus: List[str]) -> List[str]:
        """
        Filter a list of SKUs to only include purchasable ones.
        
        Args:
            skus: List of SKU strings
            
        Returns:
            Filtered list containing only purchasable SKUs
        """
        return [sku for sku in skus if self.is_purchasable(sku)]
    
    def filter_by_product_line(
        self, 
        product_line: str
    ) -> Set[str]:
        """
        Get all SKUs for a specific product line.
        
        Args:
            product_line: 'MAXimo²' or 'MAXima²'
            
        Returns:
            Set of SKUs for that product line
        """
        if 'maximo' in product_line.lower():
            return self._maximo_skus.copy()
        elif 'maxima' in product_line.lower():
            return self._maxima_skus.copy()
        return self._sku_set.copy()
    
    def get_health(self) -> Dict[str, Any]:
        """Get health check info."""
        return {
            "status": "healthy" if self.is_loaded else "unhealthy",
            "catalog_status": self._status.value,
            "product_count": len(self._products),
            "loaded_at": self._loaded_at.isoformat() if self._loaded_at else None,
            "error": self._error,
            "version": CATALOG_WIRING_VERSION,
        }


# Global singleton instance
_catalog_wiring = CatalogWiring()


def get_catalog() -> CatalogWiring:
    """
    Get the catalog wiring singleton.
    
    Usage:
        from app.catalog.wiring import get_catalog
        
        catalog = get_catalog()
        catalog.ensure_loaded()  # Raises CatalogWiringError if unavailable
        
        if catalog.is_purchasable(sku):
            # Proceed with recommendation
    """
    return _catalog_wiring


def ensure_catalog_available() -> Dict[str, Any]:
    """
    Ensure catalog is available. Call this at startup and before pipelines.
    
    Returns:
        Catalog statistics
        
    Raises:
        CatalogWiringError: If catalog is unavailable (results in 503)
    """
    catalog = get_catalog()
    return catalog.load()


def filter_to_catalog(skus: List[str]) -> List[str]:
    """
    Filter SKUs to only include those in catalog.
    
    Args:
        skus: List of SKU strings
        
    Returns:
        Filtered list
        
    Raises:
        CatalogWiringError: If catalog not loaded
    """
    catalog = get_catalog()
    if not catalog.is_loaded:
        raise CatalogWiringError(
            "CATALOG_NOT_LOADED: Catalog must be loaded before filtering. "
            "Call ensure_catalog_available() first."
        )
    return catalog.filter_skus(skus)


def get_catalog_health() -> Dict[str, Any]:
    """Get catalog health for health check endpoints."""
    return get_catalog().get_health()
