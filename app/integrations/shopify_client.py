"""
Shopify Admin API Client for GenoMAX²
=====================================
Provides authenticated access to Shopify Admin API for product sync.

Environment Variables Required:
- SHOPIFY_ADMIN_BASE_URL: Base URL (e.g., https://genomax-3.myshopify.com/admin/api/2026-01)
- SHOPIFY_ACCESS_TOKEN: Admin API access token

Usage:
    from app.integrations.shopify_client import ShopifyClient
    
    client = ShopifyClient()
    shop_info = client.get_shop()
"""

import os
import json
import time
import logging
from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass
from enum import Enum
import httpx

logger = logging.getLogger(__name__)


class ShopifyError(Exception):
    """Base exception for Shopify API errors."""
    
    def __init__(self, message: str, status_code: Optional[int] = None, response_body: Optional[Dict] = None):
        super().__init__(message)
        self.status_code = status_code
        self.response_body = response_body


class ShopifyAuthError(ShopifyError):
    """Authentication/authorization error (401/403)."""
    pass


class ShopifyRateLimitError(ShopifyError):
    """Rate limit exceeded (429)."""
    
    def __init__(self, message: str, retry_after: Optional[float] = None, **kwargs):
        super().__init__(message, **kwargs)
        self.retry_after = retry_after


class ShopifyNotFoundError(ShopifyError):
    """Resource not found (404)."""
    pass


class ShopifyValidationError(ShopifyError):
    """Validation error (422)."""
    pass


@dataclass
class RateLimitInfo:
    """Shopify API rate limit information."""
    current: int
    max: int
    retry_after: Optional[float] = None
    
    @property
    def remaining(self) -> int:
        return self.max - self.current
    
    @property
    def utilization_pct(self) -> float:
        return (self.current / self.max * 100) if self.max > 0 else 0


class ShopifyClient:
    """
    Shopify Admin API Client.
    
    Features:
    - Automatic retry on rate limits (up to 3 attempts)
    - Structured error handling
    - Rate limit tracking
    - Request/response logging
    """
    
    DEFAULT_TIMEOUT = 30.0
    MAX_RETRIES = 3
    RETRY_BACKOFF = 2.0  # seconds
    
    def __init__(
        self,
        base_url: Optional[str] = None,
        access_token: Optional[str] = None,
        timeout: float = DEFAULT_TIMEOUT,
    ):
        """
        Initialize Shopify client.
        
        Args:
            base_url: Shopify Admin API base URL (defaults to env var)
            access_token: Admin API access token (defaults to env var)
            timeout: Request timeout in seconds
        """
        self.base_url = (base_url or os.getenv("SHOPIFY_ADMIN_BASE_URL", "")).rstrip("/")
        self.access_token = access_token or os.getenv("SHOPIFY_ACCESS_TOKEN", "")
        self.timeout = timeout
        self._last_rate_limit: Optional[RateLimitInfo] = None
        
        if not self.base_url:
            logger.warning("SHOPIFY_ADMIN_BASE_URL not configured")
        if not self.access_token:
            logger.warning("SHOPIFY_ACCESS_TOKEN not configured")
    
    @property
    def is_configured(self) -> bool:
        """Check if client has required configuration."""
        return bool(self.base_url and self.access_token)
    
    @property
    def last_rate_limit(self) -> Optional[RateLimitInfo]:
        """Get rate limit info from last request."""
        return self._last_rate_limit
    
    def _get_headers(self) -> Dict[str, str]:
        """Build request headers."""
        return {
            "X-Shopify-Access-Token": self.access_token,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
    
    def _parse_rate_limit(self, headers: httpx.Headers) -> Optional[RateLimitInfo]:
        """Parse rate limit from response headers."""
        rate_limit_header = headers.get("X-Shopify-Shop-Api-Call-Limit")
        retry_after = headers.get("Retry-After")
        
        if rate_limit_header:
            try:
                current, max_calls = rate_limit_header.split("/")
                return RateLimitInfo(
                    current=int(current),
                    max=int(max_calls),
                    retry_after=float(retry_after) if retry_after else None
                )
            except (ValueError, TypeError):
                pass
        return None
    
    def _handle_response(self, response: httpx.Response) -> Dict[str, Any]:
        """Handle response and raise appropriate exceptions."""
        # Parse rate limit info
        self._last_rate_limit = self._parse_rate_limit(response.headers)
        
        # Success
        if 200 <= response.status_code < 300:
            if response.content:
                return response.json()
            return {}
        
        # Parse error body
        try:
            error_body = response.json() if response.content else {}
        except json.JSONDecodeError:
            error_body = {"raw": response.text}
        
        error_msg = error_body.get("errors", str(error_body))
        
        # Handle specific status codes
        if response.status_code == 401:
            raise ShopifyAuthError(
                f"Authentication failed: {error_msg}",
                status_code=401,
                response_body=error_body
            )
        
        if response.status_code == 403:
            raise ShopifyAuthError(
                f"Access forbidden: {error_msg}",
                status_code=403,
                response_body=error_body
            )
        
        if response.status_code == 404:
            raise ShopifyNotFoundError(
                f"Resource not found: {error_msg}",
                status_code=404,
                response_body=error_body
            )
        
        if response.status_code == 422:
            raise ShopifyValidationError(
                f"Validation error: {error_msg}",
                status_code=422,
                response_body=error_body
            )
        
        if response.status_code == 429:
            retry_after = self._last_rate_limit.retry_after if self._last_rate_limit else None
            raise ShopifyRateLimitError(
                f"Rate limit exceeded",
                status_code=429,
                response_body=error_body,
                retry_after=retry_after
            )
        
        # Generic error
        raise ShopifyError(
            f"Shopify API error ({response.status_code}): {error_msg}",
            status_code=response.status_code,
            response_body=error_body
        )
    
    def _request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict] = None,
        params: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """
        Make authenticated request to Shopify API with retry logic.
        
        Args:
            method: HTTP method (GET, POST, PUT, DELETE)
            endpoint: API endpoint (e.g., /products.json)
            data: Request body for POST/PUT
            params: Query parameters
            
        Returns:
            Response JSON as dict
        """
        if not self.is_configured:
            raise ShopifyError("Shopify client not configured. Set SHOPIFY_ADMIN_BASE_URL and SHOPIFY_ACCESS_TOKEN.")
        
        url = f"{self.base_url}{endpoint}"
        
        for attempt in range(self.MAX_RETRIES):
            try:
                with httpx.Client(timeout=self.timeout) as client:
                    response = client.request(
                        method=method,
                        url=url,
                        headers=self._get_headers(),
                        json=data,
                        params=params,
                    )
                    return self._handle_response(response)
                    
            except ShopifyRateLimitError as e:
                if attempt < self.MAX_RETRIES - 1:
                    wait_time = e.retry_after or (self.RETRY_BACKOFF * (attempt + 1))
                    logger.warning(f"Rate limited, waiting {wait_time}s (attempt {attempt + 1}/{self.MAX_RETRIES})")
                    time.sleep(wait_time)
                else:
                    raise
            except httpx.TimeoutException:
                if attempt < self.MAX_RETRIES - 1:
                    logger.warning(f"Request timeout, retrying (attempt {attempt + 1}/{self.MAX_RETRIES})")
                    time.sleep(self.RETRY_BACKOFF)
                else:
                    raise ShopifyError(f"Request timeout after {self.MAX_RETRIES} attempts")
            except httpx.RequestError as e:
                raise ShopifyError(f"Request failed: {str(e)}")
    
    # ===== Shop API =====
    
    def get_shop(self) -> Dict[str, Any]:
        """
        Get shop information (used for health check).
        
        Returns:
            Shop details including name, domain, plan, etc.
        """
        return self._request("GET", "/shop.json")
    
    # ===== Products API =====
    
    def get_product(self, product_id: int) -> Dict[str, Any]:
        """Get a single product by ID."""
        return self._request("GET", f"/products/{product_id}.json")
    
    def get_product_by_handle(self, handle: str) -> Optional[Dict[str, Any]]:
        """
        Get a product by its handle.
        
        Args:
            handle: Product handle (URL slug)
            
        Returns:
            Product dict or None if not found
        """
        try:
            result = self._request("GET", "/products.json", params={"handle": handle})
            products = result.get("products", [])
            return products[0] if products else None
        except ShopifyNotFoundError:
            return None
    
    def list_products(
        self,
        limit: int = 50,
        since_id: Optional[int] = None,
        status: Optional[str] = None,
        fields: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        List products with pagination.
        
        Args:
            limit: Max products to return (max 250)
            since_id: Return products after this ID
            status: Filter by status (active, archived, draft)
            fields: Specific fields to return
            
        Returns:
            List of product dicts
        """
        params = {"limit": min(limit, 250)}
        if since_id:
            params["since_id"] = since_id
        if status:
            params["status"] = status
        if fields:
            params["fields"] = ",".join(fields)
        
        result = self._request("GET", "/products.json", params=params)
        return result.get("products", [])
    
    def create_product(self, product_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a new product.
        
        Args:
            product_data: Product attributes (title, body_html, handle, etc.)
            
        Returns:
            Created product with ID
        """
        return self._request("POST", "/products.json", data={"product": product_data})
    
    def update_product(self, product_id: int, product_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Update an existing product.
        
        Args:
            product_id: Shopify product ID
            product_data: Fields to update
            
        Returns:
            Updated product
        """
        return self._request("PUT", f"/products/{product_id}.json", data={"product": product_data})
    
    def upsert_product_by_handle(self, handle: str, product_data: Dict[str, Any]) -> Tuple[Dict[str, Any], str]:
        """
        Create or update product by handle (idempotent).
        
        Args:
            handle: Product handle (must be set in product_data)
            product_data: Full product data
            
        Returns:
            Tuple of (product dict, action: "created" or "updated")
        """
        # Ensure handle is in product_data
        product_data["handle"] = handle
        
        # Try to find existing product
        existing = self.get_product_by_handle(handle)
        
        if existing:
            # Update existing
            product_id = existing["id"]
            result = self.update_product(product_id, product_data)
            return result.get("product", result), "updated"
        else:
            # Create new
            result = self.create_product(product_data)
            return result.get("product", result), "created"
    
    # ===== Metafields API =====
    
    def get_product_metafields(self, product_id: int, namespace: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get metafields for a product."""
        params = {}
        if namespace:
            params["namespace"] = namespace
        result = self._request("GET", f"/products/{product_id}/metafields.json", params=params)
        return result.get("metafields", [])
    
    def set_product_metafield(
        self,
        product_id: int,
        namespace: str,
        key: str,
        value: Any,
        value_type: str = "single_line_text_field",
    ) -> Dict[str, Any]:
        """
        Set a metafield on a product.
        
        Args:
            product_id: Shopify product ID
            namespace: Metafield namespace (e.g., "genomax")
            key: Metafield key
            value: Metafield value
            value_type: Shopify metafield type
            
        Returns:
            Created/updated metafield
        """
        metafield_data = {
            "namespace": namespace,
            "key": key,
            "value": str(value) if not isinstance(value, str) else value,
            "type": value_type,
        }
        return self._request(
            "POST",
            f"/products/{product_id}/metafields.json",
            data={"metafield": metafield_data}
        )
    
    def set_product_metafields_bulk(
        self,
        product_id: int,
        metafields: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Set multiple metafields on a product.
        
        Args:
            product_id: Shopify product ID
            metafields: List of metafield dicts with namespace, key, value, type
            
        Returns:
            List of created/updated metafields
        """
        results = []
        for mf in metafields:
            try:
                result = self.set_product_metafield(
                    product_id=product_id,
                    namespace=mf["namespace"],
                    key=mf["key"],
                    value=mf["value"],
                    value_type=mf.get("type", "single_line_text_field"),
                )
                results.append(result.get("metafield", result))
            except ShopifyError as e:
                logger.error(f"Failed to set metafield {mf['key']}: {e}")
                results.append({"error": str(e), "key": mf["key"]})
        return results
    
    # ===== Health Check =====
    
    def health_check(self) -> Dict[str, Any]:
        """
        Verify API connectivity and authentication.
        
        Returns:
            Health status with shop info and rate limit
        """
        if not self.is_configured:
            return {
                "ok": False,
                "error": "Client not configured",
                "configured": False,
            }
        
        try:
            shop_data = self.get_shop()
            shop = shop_data.get("shop", {})
            
            return {
                "ok": True,
                "shop": shop.get("name"),
                "domain": shop.get("myshopify_domain"),
                "plan": shop.get("plan_name"),
                "api_version": self.base_url.split("/")[-1] if "/" in self.base_url else "unknown",
                "rate_limit": {
                    "current": self._last_rate_limit.current if self._last_rate_limit else None,
                    "max": self._last_rate_limit.max if self._last_rate_limit else None,
                    "remaining": self._last_rate_limit.remaining if self._last_rate_limit else None,
                } if self._last_rate_limit else None,
            }
        except ShopifyAuthError as e:
            return {
                "ok": False,
                "error": str(e),
                "status_code": e.status_code,
                "auth_error": True,
            }
        except ShopifyError as e:
            return {
                "ok": False,
                "error": str(e),
                "status_code": e.status_code,
            }
        except Exception as e:
            return {
                "ok": False,
                "error": f"Unexpected error: {str(e)}",
            }


# Singleton instance
_client: Optional[ShopifyClient] = None


def get_shopify_client() -> ShopifyClient:
    """Get singleton Shopify client instance."""
    global _client
    if _client is None:
        _client = ShopifyClient()
    return _client
