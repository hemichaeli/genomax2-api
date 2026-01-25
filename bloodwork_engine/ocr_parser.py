"""
GenoMAX² OCR Parser Service
===========================
Google Cloud Vision integration for parsing bloodwork lab reports.

Converts OCR output to standardized {code, value, unit}[] format
for processing by BloodworkEngineV2.

Usage:
    from bloodwork_engine.ocr_parser import OCRParser, parse_bloodwork_image
    
    # Parse a base64 image
    parser = OCRParser()
    markers = parser.parse_image(base64_image_data)
    
    # Or use the convenience function
    markers = parse_bloodwork_image(image_path="/path/to/report.pdf")
"""

import re
import json
import base64
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime

# Configure logging
logger = logging.getLogger(__name__)


@dataclass
class ParsedMarker:
    """A single parsed biomarker from OCR output."""
    code: str
    value: Any
    unit: str
    confidence: float = 1.0
    raw_text: str = ""
    line_number: int = 0


@dataclass
class ParseResult:
    """Result from OCR parsing."""
    markers: List[ParsedMarker]
    raw_text: str
    parse_stats: Dict[str, int]
    lab_name: Optional[str] = None
    report_date: Optional[str] = None
    patient_name: Optional[str] = None
    errors: List[str] = None
    
    def to_dict(self) -> Dict:
        return {
            "markers": [asdict(m) for m in self.markers],
            "raw_text": self.raw_text,
            "parse_stats": self.parse_stats,
            "lab_name": self.lab_name,
            "report_date": self.report_date,
            "patient_name": self.patient_name,
            "errors": self.errors or []
        }
    
    def to_engine_input(self) -> List[Dict[str, Any]]:
        """Convert to format expected by BloodworkEngineV2.process_markers()"""
        return [
            {"code": m.code, "value": m.value, "unit": m.unit}
            for m in self.markers
        ]


class OCRParser:
    """
    Parses bloodwork lab reports using Google Cloud Vision OCR.
    
    Supports:
    - PDF documents
    - PNG/JPEG images
    - Multiple lab report formats (Quest, LabCorp, etc.)
    """
    
    # Common biomarker patterns (name -> canonical code)
    MARKER_PATTERNS = {
        # Iron/Anemia
        r"(?i)ferritin": "ferritin",
        r"(?i)iron(?:\s+serum)?": "iron",
        r"(?i)hemoglobin|hgb|hb\b": "hemoglobin",
        
        # Vitamins
        r"(?i)vitamin\s*d.*25|25.*hydroxy.*d|25-oh.*d": "vitamin_d_25oh",
        r"(?i)vitamin\s*b12|b12|cobalamin": "vitamin_b12",
        r"(?i)folate|folic\s*acid": "folate_serum",
        
        # Liver
        r"(?i)\balt\b|alanine\s*aminotransferase|sgpt": "alt",
        r"(?i)\bast\b|aspartate\s*aminotransferase|sgot": "ast",
        r"(?i)\bggt\b|gamma.*glutamyl": "ggt",
        
        # Kidney
        r"(?i)creatinine": "creatinine",
        r"(?i)egfr|estimated\s*gfr|glomerular": "egfr",
        
        # Glucose/Metabolic
        r"(?i)glucose.*fasting|fasting.*glucose|fbg": "fasting_glucose",
        r"(?i)hba1c|hemoglobin\s*a1c|glycated": "hba1c",
        r"(?i)insulin.*fasting|fasting.*insulin": "fasting_insulin",
        r"(?i)homocysteine": "homocysteine",
        
        # Inflammation
        r"(?i)hs-?crp|c-reactive|crp": "hs_crp",
        
        # Electrolytes/Minerals
        r"(?i)calcium(?:\s+serum)?": "calcium_serum",
        r"(?i)magnesium(?:\s+serum)?": "magnesium_serum",
        r"(?i)potassium|k\+": "potassium",
        r"(?i)zinc(?:\s+serum)?": "zinc_serum",
        r"(?i)copper(?:\s+serum)?": "copper_serum",
        
        # Thyroid
        r"(?i)\btsh\b|thyroid\s*stimulating": "tsh",
        r"(?i)free\s*t3|ft3": "free_t3",
        r"(?i)free\s*t4|ft4": "free_t4",
        
        # Lipids
        r"(?i)triglycerides?": "triglycerides",
        r"(?i)ldl.*cholesterol|ldl-c": "ldl_cholesterol",
        r"(?i)hdl.*cholesterol|hdl-c": "hdl_cholesterol",
        r"(?i)apolipoprotein\s*b|apo\s*b": "apolipoprotein_b",
        r"(?i)lp\(?a\)?|lipoprotein\s*a": "lp_a",
        
        # Hormones
        r"(?i)testosterone.*total|total.*testosterone": "total_testosterone",
        r"(?i)testosterone.*free|free.*testosterone": "free_testosterone",
        r"(?i)estradiol|e2\b": "estradiol",
        r"(?i)progesterone": "progesterone",
        r"(?i)cortisol": "cortisol_am",
        r"(?i)dhea-?s|dhea\s*sulfate": "dhea_s",
        r"(?i)\bshbg\b|sex\s*hormone\s*binding": "shbg",
        
        # Other
        r"(?i)uric\s*acid": "uric_acid",
        r"(?i)platelet.*count|platelets?\b": "platelet_count",
        r"(?i)omega-?3\s*index": "omega3_index",
    }
    
    # Common unit patterns
    UNIT_PATTERNS = {
        r"ng/mL": "ng/mL",
        r"ng/dL": "ng/dL",
        r"pg/mL": "pg/mL",
        r"pg/dL": "pg/dL",
        r"µg/dL|mcg/dL|ug/dL": "µg/dL",
        r"µg/L|mcg/L|ug/L": "µg/L",
        r"µmol/L|umol/L": "µmol/L",
        r"µIU/mL|uIU/mL": "µIU/mL",
        r"mIU/L": "mIU/L",
        r"mmol/L": "mmol/L",
        r"nmol/L": "nmol/L",
        r"pmol/L": "pmol/L",
        r"mg/dL": "mg/dL",
        r"mg/L": "mg/L",
        r"g/dL": "g/dL",
        r"g/L": "g/L",
        r"U/L|IU/L": "U/L",
        r"mEq/L": "mEq/L",
        r"mL/min(?:/1\.73m2)?": "mL/min/1.73m2",
        r"x10\^?3/µL|x10\^?9/L|K/µL|K/uL": "x10^3/µL",
        r"%": "%",
    }
    
    # Lab name patterns
    LAB_PATTERNS = {
        r"(?i)quest\s*diagnostics": "Quest Diagnostics",
        r"(?i)labcorp|laboratory\s*corporation": "LabCorp",
        r"(?i)bioreference": "BioReference Laboratories",
        r"(?i)sonora\s*quest": "Sonora Quest",
        r"(?i)life\s*extension": "Life Extension",
        r"(?i)inside\s*tracker": "InsideTracker",
        r"(?i)everlywell": "Everlywell",
        r"(?i)letsgetchecked": "LetsGetChecked",
    }
    
    def __init__(self, google_credentials_path: Optional[str] = None):
        """
        Initialize OCR parser.
        
        Args:
            google_credentials_path: Path to Google Cloud credentials JSON.
                                    If None, uses GOOGLE_APPLICATION_CREDENTIALS env var.
        """
        self.credentials_path = google_credentials_path
        self._client = None
    
    @property
    def client(self):
        """Lazy-load Google Cloud Vision client."""
        if self._client is None:
            try:
                from google.cloud import vision
                import os
                
                if self.credentials_path:
                    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.credentials_path
                
                self._client = vision.ImageAnnotatorClient()
                logger.info("Google Cloud Vision client initialized")
            except ImportError:
                raise ImportError(
                    "google-cloud-vision is required. "
                    "Install with: pip install google-cloud-vision"
                )
            except Exception as e:
                raise RuntimeError(f"Failed to initialize Google Cloud Vision: {e}")
        
        return self._client
    
    def parse_image(self, image_data: bytes, mime_type: str = "image/png") -> ParseResult:
        """
        Parse a bloodwork image using Google Cloud Vision OCR.
        
        Args:
            image_data: Raw image bytes or base64-encoded string
            mime_type: Image MIME type (image/png, image/jpeg, application/pdf)
        
        Returns:
            ParseResult with extracted markers
        """
        from google.cloud import vision
        
        # Decode base64 if necessary
        if isinstance(image_data, str):
            image_data = base64.b64decode(image_data)
        
        # Build vision image
        image = vision.Image(content=image_data)
        
        # Perform OCR
        if mime_type == "application/pdf":
            # For PDF, use document_text_detection
            response = self.client.document_text_detection(image=image)
        else:
            # For images, use text_detection
            response = self.client.text_detection(image=image)
        
        if response.error.message:
            raise RuntimeError(f"Google Cloud Vision error: {response.error.message}")
        
        # Extract full text
        texts = response.text_annotations
        if not texts:
            return ParseResult(
                markers=[],
                raw_text="",
                parse_stats={"total_lines": 0, "matched_markers": 0},
                errors=["No text detected in image"]
            )
        
        raw_text = texts[0].description
        
        # Parse the text
        return self._parse_text(raw_text)
    
    def parse_file(self, file_path: str) -> ParseResult:
        """
        Parse a bloodwork file (PDF or image).
        
        Args:
            file_path: Path to the file
        
        Returns:
            ParseResult with extracted markers
        """
        path = Path(file_path)
        
        if not path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        # Determine MIME type
        suffix = path.suffix.lower()
        mime_types = {
            ".pdf": "application/pdf",
            ".png": "image/png",
            ".jpg": "image/jpeg",
            ".jpeg": "image/jpeg",
            ".gif": "image/gif",
            ".webp": "image/webp",
        }
        
        mime_type = mime_types.get(suffix)
        if not mime_type:
            raise ValueError(f"Unsupported file type: {suffix}")
        
        # Read file
        with open(path, "rb") as f:
            image_data = f.read()
        
        return self.parse_image(image_data, mime_type)
    
    def _parse_text(self, raw_text: str) -> ParseResult:
        """
        Parse OCR text to extract biomarkers.
        
        Args:
            raw_text: Raw OCR text output
        
        Returns:
            ParseResult with extracted markers
        """
        markers = []
        errors = []
        lines = raw_text.split("\n")
        
        # Extract lab name
        lab_name = self._detect_lab(raw_text)
        
        # Extract report date
        report_date = self._extract_date(raw_text)
        
        # Process each line
        for i, line in enumerate(lines):
            line = line.strip()
            if not line:
                continue
            
            # Try to match marker pattern
            marker = self._parse_line(line, i + 1)
            if marker:
                markers.append(marker)
        
        # Deduplicate markers (keep highest confidence)
        markers = self._deduplicate_markers(markers)
        
        parse_stats = {
            "total_lines": len(lines),
            "non_empty_lines": len([l for l in lines if l.strip()]),
            "matched_markers": len(markers),
        }
        
        return ParseResult(
            markers=markers,
            raw_text=raw_text,
            parse_stats=parse_stats,
            lab_name=lab_name,
            report_date=report_date,
            errors=errors if errors else None
        )
    
    def _parse_line(self, line: str, line_number: int) -> Optional[ParsedMarker]:
        """
        Parse a single line to extract a biomarker.
        
        Args:
            line: Text line from OCR
            line_number: Line number for debugging
        
        Returns:
            ParsedMarker if found, None otherwise
        """
        # Try to match marker name
        matched_code = None
        for pattern, code in self.MARKER_PATTERNS.items():
            if re.search(pattern, line):
                matched_code = code
                break
        
        if not matched_code:
            return None
        
        # Try to extract value and unit
        value, unit, confidence = self._extract_value_unit(line)
        
        if value is None:
            return None
        
        return ParsedMarker(
            code=matched_code,
            value=value,
            unit=unit,
            confidence=confidence,
            raw_text=line,
            line_number=line_number
        )
    
    def _extract_value_unit(self, line: str) -> Tuple[Optional[Any], str, float]:
        """
        Extract numeric value and unit from a line.
        
        Returns:
            Tuple of (value, unit, confidence)
        """
        # Common patterns for value extraction
        # Pattern 1: value unit (e.g., "45.2 ng/mL")
        # Pattern 2: value  range (e.g., "45.2  30-100")
        # Pattern 3: result: value (e.g., "Result: 45.2")
        
        value = None
        unit = "unknown"
        confidence = 0.5
        
        # Try to find a numeric value
        # Handle values like: 45.2, <0.5, >100, 1,234.5
        value_pattern = r"(?:(?:result|value|level)[\s:]+)?([<>]?\s*[\d,]+\.?\d*)"
        value_match = re.search(value_pattern, line, re.IGNORECASE)
        
        if value_match:
            raw_value = value_match.group(1).replace(",", "").strip()
            
            # Handle < and > prefixes
            if raw_value.startswith("<"):
                # Use half the threshold for "below detection"
                try:
                    value = float(raw_value[1:].strip()) / 2
                    confidence = 0.7
                except ValueError:
                    return None, "", 0.0
            elif raw_value.startswith(">"):
                # Use 1.1x the threshold for "above detection"
                try:
                    value = float(raw_value[1:].strip()) * 1.1
                    confidence = 0.7
                except ValueError:
                    return None, "", 0.0
            else:
                try:
                    value = float(raw_value)
                    confidence = 0.9
                except ValueError:
                    return None, "", 0.0
        
        # Try to find unit
        for pattern, canonical_unit in self.UNIT_PATTERNS.items():
            if re.search(pattern, line, re.IGNORECASE):
                unit = canonical_unit
                confidence = min(confidence + 0.1, 1.0)
                break
        
        return value, unit, confidence
    
    def _detect_lab(self, text: str) -> Optional[str]:
        """Detect lab name from text."""
        for pattern, lab_name in self.LAB_PATTERNS.items():
            if re.search(pattern, text):
                return lab_name
        return None
    
    def _extract_date(self, text: str) -> Optional[str]:
        """Extract report date from text."""
        # Common date patterns
        date_patterns = [
            r"(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})",  # MM/DD/YYYY or DD-MM-YYYY
            r"(\d{4}[/-]\d{1,2}[/-]\d{1,2})",     # YYYY-MM-DD
            r"((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{1,2},?\s+\d{4})",  # Month DD, YYYY
        ]
        
        for pattern in date_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1)
        
        return None
    
    def _deduplicate_markers(self, markers: List[ParsedMarker]) -> List[ParsedMarker]:
        """Deduplicate markers, keeping highest confidence for each code."""
        by_code = {}
        for marker in markers:
            if marker.code not in by_code or marker.confidence > by_code[marker.code].confidence:
                by_code[marker.code] = marker
        return list(by_code.values())


# Convenience functions

def parse_bloodwork_image(
    image_data: Optional[bytes] = None,
    image_path: Optional[str] = None,
    credentials_path: Optional[str] = None
) -> ParseResult:
    """
    Parse a bloodwork image or file.
    
    Args:
        image_data: Raw image bytes (provide this OR image_path)
        image_path: Path to image file (provide this OR image_data)
        credentials_path: Optional path to Google Cloud credentials
    
    Returns:
        ParseResult with extracted markers
    """
    parser = OCRParser(google_credentials_path=credentials_path)
    
    if image_path:
        return parser.parse_file(image_path)
    elif image_data:
        return parser.parse_image(image_data)
    else:
        raise ValueError("Must provide either image_data or image_path")


def parse_text_fallback(raw_text: str) -> ParseResult:
    """
    Parse raw text without OCR (for already-extracted text or testing).
    
    Args:
        raw_text: OCR text or manually entered text
    
    Returns:
        ParseResult with extracted markers
    """
    parser = OCRParser()
    return parser._parse_text(raw_text)
