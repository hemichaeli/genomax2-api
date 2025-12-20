"""
GenoMAX2 Canonical Hashing Layer
Single source of truth for all hash operations.
"""

import hashlib
import json
from typing import Dict, Any, Optional

# Fields to exclude from hashing (volatile/generated)
VOLATILE_FIELDS = frozenset([
    "created_at",
    "updated_at", 
    "timestamp",
    "id",
    "run_id",
    "_metadata"
])


def canonicalize(obj: Any, exclude_volatile: bool = True) -> str:
    """
    Convert object to canonical JSON string.
    Deterministic: same input always produces same output.
    """
    def _clean(o: Any) -> Any:
        if isinstance(o, dict):
            return {
                k: _clean(v) 
                for k, v in sorted(o.items()) 
                if not (exclude_volatile and k in VOLATILE_FIELDS)
            }
        elif isinstance(o, (list, tuple)):
            return [_clean(i) for i in o]
        elif isinstance(o, float):
            # Normalize floats to avoid precision issues
            return round(o, 10)
        return o
    
    cleaned = _clean(obj)
    return json.dumps(cleaned, sort_keys=True, separators=(',', ':'), ensure_ascii=True)


def canonicalize_and_hash(obj: Any, exclude_volatile: bool = True) -> str:
    """
    THE canonical hash function for all GenoMAX2 systems.
    Returns: "sha256:<64-char-hex>"
    """
    canonical = canonicalize(obj, exclude_volatile)
    digest = hashlib.sha256(canonical.encode('utf-8')).hexdigest()
    return f"sha256:{digest}"


def verify_hash(obj: Any, expected_hash: str, exclude_volatile: bool = True) -> bool:
    """
    Verify object matches expected hash.
    """
    computed = canonicalize_and_hash(obj, exclude_volatile)
    return computed == expected_hash


def extract_hash_digest(full_hash: str) -> str:
    """
    Extract raw digest from prefixed hash.
    "sha256:abc123..." -> "abc123..."
    """
    if full_hash.startswith("sha256:"):
        return full_hash[7:]
    return full_hash
