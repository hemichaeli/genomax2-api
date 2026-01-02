"""
GenoMAX² Safety Gate v1.0
Ingredient-Level Safety Enforcement for the Brain Resolver

This module provides database-driven safety blocking based on:
1. Rejected ingredients (tier_classification = 'TIER 3 (REJECTED)')
2. Module-to-ingredient linkage (module_ingredient_links table)

The safety gate enforces "Blood does not negotiate" by ensuring
that modules containing rejected ingredients are NEVER recommended,
regardless of other scoring or intent matching.

Usage:
    from app.brain.safety_gate import (
        get_safety_blocked_ingredients,
        check_modules_safety,
        apply_safety_gate_to_constraints,
    )
    
    # Get all globally blocked ingredients
    blocked = get_safety_blocked_ingredients(conn)
    
    # Check specific modules
    results = check_modules_safety(conn, ['MET-ASHWAG-M-098', 'VIT-D3K2-M-001'])
    
    # Apply safety gate to existing constraints
    enhanced = apply_safety_gate_to_constraints(conn, routing_constraints)

Database Requirements:
    - ingredients table with safety_status, rejection_reason columns
    - module_ingredient_links table linking modules to ingredients
    - os_modules_v3_1 table for module metadata

IMPORTANT: Safety gate is DETERMINISTIC and NON-NEGOTIABLE.
Rejected ingredients = permanent block. No override allowed.
"""

from __future__ import annotations
from typing import Dict, List, Tuple, Optional, Any, Set
from dataclasses import dataclass
from datetime import datetime, timezone
import json


# =============================================================================
# SAFETY GATE VERSION
# =============================================================================
SAFETY_GATE_VERSION = "1.0.0"


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class RejectedIngredient:
    """A permanently rejected ingredient."""
    ingredient_id: int
    name: str
    tier_classification: str
    rejection_reason: str
    rejection_date: Optional[str] = None


@dataclass
class ModuleSafetyResult:
    """Safety check result for a single module."""
    module_code: str
    status: str  # 'PASS' or 'BLOCKED'
    blocked_by: Optional[str] = None  # Ingredient name that caused block
    reason: Optional[str] = None  # Rejection reason


@dataclass
class SafetyGateOutput:
    """Complete safety gate output."""
    gate_version: str
    checked_at: str
    blocked_ingredients: List[str]
    blocked_modules: List[str]
    module_results: List[ModuleSafetyResult]
    rejected_ingredient_details: Dict[str, RejectedIngredient]


# =============================================================================
# DATABASE QUERIES
# =============================================================================

SQL_GET_REJECTED_INGREDIENTS = """
SELECT 
    id as ingredient_id,
    name,
    tier_classification,
    safety_status,
    rejection_reason,
    rejection_date
FROM ingredients
WHERE safety_status = 'REJECTED'
ORDER BY name;
"""

SQL_CHECK_MODULES_SAFETY = """
SELECT 
    m.module_code,
    CASE WHEN i.safety_status = 'REJECTED' THEN 'BLOCKED' ELSE 'PASS' END as status,
    i.name as blocked_by,
    i.rejection_reason as reason
FROM os_modules_v3_1 m
LEFT JOIN module_ingredient_links l ON m.module_code = l.module_code
LEFT JOIN ingredients i ON l.ingredient_id = i.id AND i.safety_status = 'REJECTED'
WHERE m.module_code = ANY(%s);
"""

SQL_GET_BLOCKED_MODULES = """
SELECT DISTINCT
    m.module_code,
    m.product_name,
    i.name as blocked_by,
    i.rejection_reason
FROM os_modules_v3_1 m
JOIN module_ingredient_links l ON m.module_code = l.module_code
JOIN ingredients i ON l.ingredient_id = i.id
WHERE i.safety_status = 'REJECTED'
ORDER BY m.module_code;
"""

SQL_GET_BLOCKED_INGREDIENT_NAMES = """
SELECT DISTINCT LOWER(name) as ingredient_name
FROM ingredients
WHERE safety_status = 'REJECTED';
"""


# =============================================================================
# CORE FUNCTIONS
# =============================================================================

def now_iso() -> str:
    """Current UTC timestamp in ISO format."""
    return datetime.now(timezone.utc).isoformat()


def get_safety_blocked_ingredients(conn) -> List[RejectedIngredient]:
    """
    Get all ingredients with REJECTED safety status.
    
    These ingredients are PERMANENTLY blocked from all recommendations.
    
    Args:
        conn: Database connection (psycopg2)
        
    Returns:
        List of RejectedIngredient objects
    """
    cur = conn.cursor()
    try:
        cur.execute(SQL_GET_REJECTED_INGREDIENTS)
        rows = cur.fetchall()
        
        results = []
        for row in rows:
            results.append(RejectedIngredient(
                ingredient_id=row['ingredient_id'],
                name=row['name'],
                tier_classification=row['tier_classification'],
                rejection_reason=row['rejection_reason'] or 'Safety concern',
                rejection_date=str(row['rejection_date']) if row.get('rejection_date') else None
            ))
        
        return results
    finally:
        cur.close()


def get_blocked_ingredient_names(conn) -> Set[str]:
    """
    Get lowercase names of all rejected ingredients.
    
    Used for blocked_ingredients list in RoutingConstraints.
    
    Args:
        conn: Database connection
        
    Returns:
        Set of lowercase ingredient names
    """
    cur = conn.cursor()
    try:
        cur.execute(SQL_GET_BLOCKED_INGREDIENT_NAMES)
        rows = cur.fetchall()
        return {row['ingredient_name'] for row in rows}
    finally:
        cur.close()


def get_all_blocked_modules(conn) -> List[Dict[str, Any]]:
    """
    Get all modules blocked due to rejected ingredients.
    
    Args:
        conn: Database connection
        
    Returns:
        List of dicts with module_code, product_name, blocked_by, rejection_reason
    """
    cur = conn.cursor()
    try:
        cur.execute(SQL_GET_BLOCKED_MODULES)
        return [dict(row) for row in cur.fetchall()]
    finally:
        cur.close()


def check_modules_safety(
    conn,
    module_codes: List[str]
) -> List[ModuleSafetyResult]:
    """
    Check safety status for specific modules.
    
    This is the main entry point for the Brain resolver to
    validate candidate modules before recommendation.
    
    Args:
        conn: Database connection
        module_codes: List of module codes to check
        
    Returns:
        List of ModuleSafetyResult (one per input module)
    """
    if not module_codes:
        return []
    
    cur = conn.cursor()
    try:
        cur.execute(SQL_CHECK_MODULES_SAFETY, (module_codes,))
        rows = cur.fetchall()
        
        # Build result map
        results_map: Dict[str, ModuleSafetyResult] = {}
        
        for row in rows:
            code = row['module_code']
            status = row['status']
            
            # If any link shows BLOCKED, the module is blocked
            if code in results_map:
                if status == 'BLOCKED':
                    results_map[code] = ModuleSafetyResult(
                        module_code=code,
                        status='BLOCKED',
                        blocked_by=row['blocked_by'],
                        reason=row['reason']
                    )
            else:
                results_map[code] = ModuleSafetyResult(
                    module_code=code,
                    status=status,
                    blocked_by=row['blocked_by'] if status == 'BLOCKED' else None,
                    reason=row['reason'] if status == 'BLOCKED' else None
                )
        
        # Fill in any modules not in database (default to PASS)
        for code in module_codes:
            if code not in results_map:
                results_map[code] = ModuleSafetyResult(
                    module_code=code,
                    status='PASS',
                    blocked_by=None,
                    reason=None
                )
        
        # Return in input order
        return [results_map[code] for code in module_codes]
        
    finally:
        cur.close()


def apply_safety_gate_to_constraints(
    conn,
    routing_constraints: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Enhance routing constraints with safety gate data.
    
    This function:
    1. Fetches all rejected ingredients from database
    2. Adds them to blocked_ingredients list
    3. Returns enhanced constraints
    
    The resolver should call this AFTER merging bloodwork/lifestyle
    constraints but BEFORE routing.
    
    Args:
        conn: Database connection
        routing_constraints: Existing RoutingConstraints dict
        
    Returns:
        Enhanced RoutingConstraints dict with safety blocks applied
    """
    # Get blocked ingredient names from database
    blocked_names = get_blocked_ingredient_names(conn)
    
    # Get existing blocked ingredients from constraints
    existing_blocked = set(routing_constraints.get('blocked_ingredients', []))
    
    # Merge (union)
    merged_blocked = sorted(existing_blocked | blocked_names)
    
    # Get rejected ingredient details for audit
    rejected_ingredients = get_safety_blocked_ingredients(conn)
    
    # Build enhanced constraints
    enhanced = dict(routing_constraints)
    enhanced['blocked_ingredients'] = merged_blocked
    
    # Add safety gate metadata
    enhanced['safety_gate'] = {
        'version': SAFETY_GATE_VERSION,
        'applied_at': now_iso(),
        'rejected_ingredients_count': len(rejected_ingredients),
        'rejected_ingredients': [
            {
                'name': ri.name,
                'reason': ri.rejection_reason
            }
            for ri in rejected_ingredients
        ]
    }
    
    return enhanced


def filter_modules_by_safety(
    conn,
    candidate_modules: List[str]
) -> Tuple[List[str], List[Dict[str, str]]]:
    """
    Filter candidate modules, removing those blocked by safety gate.
    
    Args:
        conn: Database connection
        candidate_modules: List of candidate module codes
        
    Returns:
        Tuple of (allowed_modules, blocked_module_details)
    """
    if not candidate_modules:
        return [], []
    
    results = check_modules_safety(conn, candidate_modules)
    
    allowed = []
    blocked = []
    
    for result in results:
        if result.status == 'PASS':
            allowed.append(result.module_code)
        else:
            blocked.append({
                'module_code': result.module_code,
                'blocked_by': result.blocked_by,
                'reason': result.reason
            })
    
    return allowed, blocked


def build_safety_gate_output(
    conn,
    module_codes: Optional[List[str]] = None
) -> SafetyGateOutput:
    """
    Build complete safety gate output for audit/debugging.
    
    Args:
        conn: Database connection
        module_codes: Optional list of modules to check (checks all if None)
        
    Returns:
        SafetyGateOutput with full details
    """
    # Get rejected ingredients
    rejected = get_safety_blocked_ingredients(conn)
    rejected_dict = {ri.name: ri for ri in rejected}
    blocked_ingredient_names = [ri.name.lower() for ri in rejected]
    
    # Get blocked modules
    blocked_modules_data = get_all_blocked_modules(conn)
    blocked_module_codes = [m['module_code'] for m in blocked_modules_data]
    
    # Check specific modules if provided
    if module_codes:
        module_results = check_modules_safety(conn, module_codes)
    else:
        module_results = []
    
    return SafetyGateOutput(
        gate_version=SAFETY_GATE_VERSION,
        checked_at=now_iso(),
        blocked_ingredients=blocked_ingredient_names,
        blocked_modules=blocked_module_codes,
        module_results=module_results,
        rejected_ingredient_details=rejected_dict
    )


# =============================================================================
# INTEGRATION WITH BRAIN RESOLVER
# =============================================================================

def integrate_safety_gate_with_resolver(
    conn,
    resolved_constraints: Dict[str, Any],
    candidate_modules: List[str]
) -> Tuple[Dict[str, Any], List[str], List[Dict]]:
    """
    Full integration point for Brain resolver.
    
    Call this after resolve_all() and before routing to:
    1. Apply safety gate to constraints
    2. Filter candidate modules
    3. Return enhanced data
    
    Args:
        conn: Database connection
        resolved_constraints: Output from resolver
        candidate_modules: Modules being considered for recommendation
        
    Returns:
        Tuple of:
        - Enhanced constraints with safety gate
        - Allowed modules (safety passed)
        - Blocked module details (for audit)
    """
    # Apply safety gate to constraints
    enhanced_constraints = apply_safety_gate_to_constraints(conn, resolved_constraints)
    
    # Filter modules
    allowed, blocked = filter_modules_by_safety(conn, candidate_modules)
    
    return enhanced_constraints, allowed, blocked


# =============================================================================
# EXAMPLE USAGE
# =============================================================================

if __name__ == "__main__":
    # Example usage (requires database connection)
    print("Safety Gate Module v" + SAFETY_GATE_VERSION)
    print("This module integrates with the Brain resolver for ingredient safety.")
    print()
    print("Key functions:")
    print("  - get_safety_blocked_ingredients(conn)")
    print("  - check_modules_safety(conn, module_codes)")
    print("  - apply_safety_gate_to_constraints(conn, constraints)")
    print("  - filter_modules_by_safety(conn, modules)")
    print("  - integrate_safety_gate_with_resolver(conn, constraints, modules)")
