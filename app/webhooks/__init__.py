"""
GenoMAX² Webhook Infrastructure
Lab Integration Receivers for Junction (Vital) and Lab Testing API

Version: 1.0.0
"""

from .receiver import router as webhook_router
from .models import (
    JunctionWebhookPayload,
    LabTestingAPIWebhookPayload,
    WebhookEvent,
    LabResult,
    Biomarker
)
from .processor import WebhookProcessor

__all__ = [
    "webhook_router",
    "JunctionWebhookPayload",
    "LabTestingAPIWebhookPayload",
    "WebhookEvent",
    "LabResult",
    "Biomarker",
    "WebhookProcessor"
]
