"""
Copy Renderer for GenoMAX²
==========================
Deterministic rendering functions for front/back labels and Shopify body.

Rules:
- NO marketing hype or disease claims
- NO invented dosing amounts
- ONLY use existing DB fields
- Output must contain ZERO placeholder tokens (TBD/MISSING/REVIEW/PLACEHOLDER)
"""

import re
from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass
from enum import Enum


# Placeholder pattern - must match exactly what Shopify gate uses
PLACEHOLDER_PATTERN = re.compile(
    r'\b(TBD|MISSING|REVIEW|PLACEHOLDER)\b',
    re.IGNORECASE
)


class OSLayer(str, Enum):
    """OS Layer mapping to human-readable role labels."""
    CORE = "Core"
    ADAPTIVE = "Adaptive"
    SUPPORT = "Support"


# Map os_layer values to role labels
OS_LAYER_TO_ROLE = {
    "Core": "Core System Module",
    "Adaptive": "Adaptive Module",
    "Support": "Support Module",
    "CORE": "Core System Module",
    "ADAPTIVE": "Adaptive Module",
    "SUPPORT": "Support Module",
}


@dataclass
class RenderResult:
    """Result of a render operation."""
    success: bool
    content: str
    warnings: List[str]
    has_placeholders: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "success": self.success,
            "content": self.content,
            "warnings": self.warnings,
            "has_placeholders": self.has_placeholders,
        }


def contains_placeholder(text: Optional[str]) -> bool:
    """Check if text contains any placeholder tokens."""
    if not text:
        return False
    return bool(PLACEHOLDER_PATTERN.search(text))


def find_placeholders(text: Optional[str]) -> List[str]:
    """Find all placeholder tokens in text."""
    if not text:
        return []
    return PLACEHOLDER_PATTERN.findall(text)


def strip_placeholders(text: Optional[str]) -> str:
    """Remove placeholder tokens from text (for cleanup)."""
    if not text:
        return ""
    # Replace placeholders with empty string and clean up extra whitespace
    cleaned = PLACEHOLDER_PATTERN.sub("", text)
    # Clean up multiple spaces/newlines
    cleaned = re.sub(r'\s+', ' ', cleaned)
    cleaned = re.sub(r'^\s+|\s+$', '', cleaned)
    return cleaned


def render_os_role(os_layer: Optional[str]) -> str:
    """Convert os_layer to human-readable role label."""
    if not os_layer:
        return "System Module"
    return OS_LAYER_TO_ROLE.get(os_layer, "System Module")


def render_front_label(module: Dict[str, Any]) -> RenderResult:
    """
    Render front label copy from module data.
    
    Must include:
    - GenoMAX² brand line (system-first, no claims)
    - product_name
    - os_environment badge (MAXimo²/MAXima²)
    - os_role label derived from os_layer
    - net_quantity (raw)
    
    Must NOT include:
    - biological_domain raw jargon
    - contraindications/drug interactions
    """
    warnings = []
    
    # Required fields
    product_name = module.get("product_name")
    if not product_name:
        return RenderResult(
            success=False,
            content="",
            warnings=["Missing required field: product_name"],
            has_placeholders=False,
        )
    
    # Strip any existing placeholders from product_name
    product_name = strip_placeholders(product_name)
    if not product_name:
        return RenderResult(
            success=False,
            content="",
            warnings=["product_name is empty after removing placeholders"],
            has_placeholders=False,
        )
    
    os_environment = module.get("os_environment", "")
    os_layer = module.get("os_layer", "")
    net_quantity = module.get("net_quantity", "")
    
    # Build front label components
    lines = []
    
    # Line 1: Brand + Environment
    if os_environment:
        lines.append(f"GenoMAX² | {os_environment}")
    else:
        lines.append("GenoMAX²")
    
    # Line 2: Product Name
    lines.append(product_name)
    
    # Line 3: OS Role
    os_role = render_os_role(os_layer)
    lines.append(os_role)
    
    # Line 4: Net Quantity (if available)
    if net_quantity:
        # Strip placeholders from net_quantity too
        net_qty_clean = strip_placeholders(str(net_quantity))
        if net_qty_clean:
            lines.append(net_qty_clean)
        else:
            warnings.append("net_quantity empty after cleanup")
    else:
        warnings.append("net_quantity not provided")
    
    content = "\n".join(lines)
    
    # Final validation
    has_placeholders = contains_placeholder(content)
    if has_placeholders:
        warnings.append(f"Generated content still contains placeholders: {find_placeholders(content)}")
    
    return RenderResult(
        success=not has_placeholders,
        content=content,
        warnings=warnings,
        has_placeholders=has_placeholders,
    )


def render_back_label(module: Dict[str, Any]) -> RenderResult:
    """
    Render back label copy from module data.
    
    Must include (if present):
    - Suggested Use (suggested_use_full)
    - Safety Notes (safety_notes)
    - Contraindications (contraindications)
    - Ingredients text (ingredients_raw_text)
    - FDA disclaimer block (fda_disclaimer)
    
    If any section missing, omit cleanly (no placeholders).
    """
    warnings = []
    sections = []
    
    # Suggested Use
    suggested_use = module.get("suggested_use_full")
    if suggested_use:
        suggested_use_clean = strip_placeholders(suggested_use)
        if suggested_use_clean:
            sections.append(f"Suggested Use: {suggested_use_clean}")
        else:
            warnings.append("suggested_use_full empty after cleanup")
    
    # Safety Notes
    safety_notes = module.get("safety_notes")
    if safety_notes:
        safety_clean = strip_placeholders(safety_notes)
        if safety_clean:
            sections.append(f"Safety Notes: {safety_clean}")
        else:
            warnings.append("safety_notes empty after cleanup")
    
    # Contraindications
    contraindications = module.get("contraindications")
    if contraindications:
        contra_clean = strip_placeholders(contraindications)
        if contra_clean:
            sections.append(f"Contraindications: {contra_clean}")
        else:
            warnings.append("contraindications empty after cleanup")
    
    # Ingredients
    ingredients = module.get("ingredients_raw_text")
    if ingredients:
        ingredients_clean = strip_placeholders(ingredients)
        if ingredients_clean:
            sections.append(f"Ingredients: {ingredients_clean}")
        else:
            warnings.append("ingredients_raw_text empty after cleanup")
    
    # FDA Disclaimer (required for Shopify gate)
    fda_disclaimer = module.get("fda_disclaimer")
    if fda_disclaimer:
        fda_clean = strip_placeholders(fda_disclaimer)
        if fda_clean:
            sections.append(fda_clean)
        else:
            warnings.append("fda_disclaimer empty after cleanup")
    else:
        warnings.append("fda_disclaimer not provided")
    
    # Build content
    if not sections:
        return RenderResult(
            success=False,
            content="",
            warnings=["No content available for back label"],
            has_placeholders=False,
        )
    
    content = "\n\n".join(sections)
    
    # Final validation
    has_placeholders = contains_placeholder(content)
    if has_placeholders:
        warnings.append(f"Generated content still contains placeholders: {find_placeholders(content)}")
    
    return RenderResult(
        success=not has_placeholders,
        content=content,
        warnings=warnings,
        has_placeholders=has_placeholders,
    )


def render_shopify_body(module: Dict[str, Any]) -> RenderResult:
    """
    Render Shopify body HTML from module data.
    
    Uses back_label_text if shopify_body missing.
    Formats as simple HTML with sections.
    Appends FDA disclaimer at end.
    """
    warnings = []
    
    # Check if shopify_body already exists and is clean
    existing_body = module.get("shopify_body")
    if existing_body and not contains_placeholder(existing_body):
        return RenderResult(
            success=True,
            content=existing_body,
            warnings=[],
            has_placeholders=False,
        )
    
    # Build from components
    html_sections = []
    
    # Product description (use product_name as header if available)
    product_name = module.get("product_name")
    if product_name:
        product_name_clean = strip_placeholders(product_name)
        if product_name_clean:
            html_sections.append(f"<h2>{product_name_clean}</h2>")
    
    # OS Environment badge
    os_environment = module.get("os_environment")
    os_layer = module.get("os_layer")
    if os_environment or os_layer:
        badge_parts = []
        if os_environment:
            badge_parts.append(os_environment)
        if os_layer:
            badge_parts.append(render_os_role(os_layer))
        html_sections.append(f"<p><strong>GenoMAX² {' | '.join(badge_parts)}</strong></p>")
    
    # Suggested Use
    suggested_use = module.get("suggested_use_full")
    if suggested_use:
        suggested_clean = strip_placeholders(suggested_use)
        if suggested_clean:
            html_sections.append(f"<h3>Suggested Use</h3>\n<p>{suggested_clean}</p>")
        else:
            warnings.append("suggested_use_full empty after cleanup")
    
    # Safety Notes
    safety_notes = module.get("safety_notes")
    if safety_notes:
        safety_clean = strip_placeholders(safety_notes)
        if safety_clean:
            html_sections.append(f"<h3>Safety Notes</h3>\n<p>{safety_clean}</p>")
        else:
            warnings.append("safety_notes empty after cleanup")
    
    # Contraindications
    contraindications = module.get("contraindications")
    if contraindications:
        contra_clean = strip_placeholders(contraindications)
        if contra_clean:
            html_sections.append(f"<h3>Contraindications</h3>\n<p>{contra_clean}</p>")
        else:
            warnings.append("contraindications empty after cleanup")
    
    # Ingredients
    ingredients = module.get("ingredients_raw_text")
    if ingredients:
        ingredients_clean = strip_placeholders(ingredients)
        if ingredients_clean:
            html_sections.append(f"<h3>Ingredients</h3>\n<p>{ingredients_clean}</p>")
        else:
            warnings.append("ingredients_raw_text empty after cleanup")
    
    # FDA Disclaimer (always at end)
    fda_disclaimer = module.get("fda_disclaimer")
    if fda_disclaimer:
        fda_clean = strip_placeholders(fda_disclaimer)
        if fda_clean:
            html_sections.append(f"<p><em>{fda_clean}</em></p>")
        else:
            warnings.append("fda_disclaimer empty after cleanup")
    else:
        warnings.append("fda_disclaimer not provided")
    
    # Build content
    if not html_sections:
        return RenderResult(
            success=False,
            content="",
            warnings=["No content available for Shopify body"],
            has_placeholders=False,
        )
    
    content = "\n\n".join(html_sections)
    
    # Final validation
    has_placeholders = contains_placeholder(content)
    if has_placeholders:
        warnings.append(f"Generated content still contains placeholders: {find_placeholders(content)}")
    
    return RenderResult(
        success=not has_placeholders,
        content=content,
        warnings=warnings,
        has_placeholders=has_placeholders,
    )


def render_all(module: Dict[str, Any]) -> Dict[str, RenderResult]:
    """
    Render all copy fields for a module.
    
    Returns dict with results for each field.
    """
    return {
        "front_label_text": render_front_label(module),
        "back_label_text": render_back_label(module),
        "shopify_body": render_shopify_body(module),
    }


def analyze_module_placeholders(module: Dict[str, Any]) -> Dict[str, Any]:
    """
    Analyze a module for placeholder tokens.
    
    Returns detailed breakdown of which fields contain which placeholders.
    """
    fields_to_check = [
        "front_label_text",
        "back_label_text",
        "shopify_body",
        "product_name",
    ]
    
    result = {
        "module_code": module.get("module_code"),
        "shopify_handle": module.get("shopify_handle"),
        "has_placeholders": False,
        "fields_with_placeholders": {},
        "total_placeholder_count": 0,
    }
    
    for field in fields_to_check:
        value = module.get(field)
        if value:
            placeholders = find_placeholders(value)
            if placeholders:
                result["has_placeholders"] = True
                result["fields_with_placeholders"][field] = {
                    "tokens": placeholders,
                    "count": len(placeholders),
                }
                result["total_placeholder_count"] += len(placeholders)
    
    return result
