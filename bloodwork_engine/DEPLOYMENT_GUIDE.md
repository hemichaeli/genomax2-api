# GenoMAX² v3.33.0 Lab Integration - Deployment Guide

**Date:** January 28, 2026  
**Version:** 3.33.0  
**Status:** Ready for deployment

## Quick Start Checklist

- [ ] 1. Execute database migration
- [ ] 2. Set environment variables in Railway
- [ ] 3. Schedule Junction intro call
- [ ] 4. Set up Google Cloud Storage + Vision API
- [ ] 5. Test endpoints

---

## 1. Deploy Code to Railway

Code is now deployed via GitHub. Railway will auto-deploy on push to main.

Deployed files in `bloodwork_engine/`:
- `lab_upload.py` - OCR upload endpoint
- `junction_client.py` - Junction/Vital API client
- `bloodwork_brain.py` - Brain handoff pipeline
- `lab_integration_router.py` - Central router
- `tests/test_lab_integration.py` - Test suite
- `requirements_lab_integration.txt` - Dependencies

---

## 2. Execute Database Migration

### Option A: Railway Data Tab
1. Go to Railway Dashboard > GenoMAX² > Postgres
2. Click "Data" tab
3. Click "Query"
4. Copy/paste contents of `V3.33.0__lab_integration_tables.sql`
5. Execute

### Option B: psql Command Line
```bash
# Get DATABASE_URL from Railway
# Railway Dashboard > GenoMAX² > Postgres > Variables > DATABASE_URL

# Execute migration
psql "postgresql://postgres:PASSWORD@HOST:PORT/railway" -f V3.33.0__lab_integration_tables.sql
```

### Verify Migration
```sql
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'public' 
AND table_name IN ('lab_orders', 'bloodwork_submissions', 'biomarker_reference_ranges');

-- Should return 3 rows

SELECT code, display_name, priority_tier 
FROM biomarker_reference_ranges 
ORDER BY priority_tier, code;

-- Should return 13 priority biomarkers
```

---

## 3. Set Environment Variables in Railway

Go to Railway Dashboard > GenoMAX² > web > Variables

Add these variables:

```
# Junction/Vital (after partnership call)
JUNCTION_API_KEY=vital_sk_...
JUNCTION_WEBHOOK_SECRET=whsec_...
JUNCTION_ENVIRONMENT=sandbox

# Google Cloud (for OCR)
GOOGLE_APPLICATION_CREDENTIALS=/app/service-account.json
GCS_BUCKET_NAME=genomax2-lab-uploads

# Anthropic (may already exist)
ANTHROPIC_API_KEY=sk-ant-...
```

---

## 4. Set Up Google Cloud Storage

### Create Bucket
```bash
gcloud storage buckets create gs://genomax2-lab-uploads \
  --location=us-central1 \
  --uniform-bucket-level-access

# Set lifecycle (delete after 90 days)
gcloud storage buckets update gs://genomax2-lab-uploads \
  --lifecycle-file=lifecycle.json
```

### Enable Vision API
```bash
gcloud services enable vision.googleapis.com
```

### Create Service Account
```bash
gcloud iam service-accounts create genomax2-lab-ocr \
  --display-name="GenoMAX2 Lab OCR"

gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="serviceAccount:genomax2-lab-ocr@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"

gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="serviceAccount:genomax2-lab-ocr@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/vision.user"

gcloud iam service-accounts keys create service-account.json \
  --iam-account=genomax2-lab-ocr@YOUR_PROJECT_ID.iam.gserviceaccount.com
```

Upload `service-account.json` to Railway as a secret file or base64-encode it.

---

## 5. Schedule Junction Call

**URL:** https://www.tryvital.io/labs

Key questions for the call:

1. Pricing tiers for 50-200 tests/month?
2. Sandbox environment timeline?
3. State coverage (especially NY, NJ, RI)?
4. SDK/sample code availability?
5. Webhook delivery guarantees?

---

## 6. Test the Endpoints

After deployment:

```bash
# Health check
curl https://YOUR_RAILWAY_URL/api/v1/lab/health

# Capabilities
curl https://YOUR_RAILWAY_URL/api/v1/lab/capabilities

# Test upload endpoint
curl -X POST https://YOUR_RAILWAY_URL/api/v1/lab/upload \
  -H "Content-Type: multipart/form-data" \
  -F "file=@sample_blood_test.pdf"

# Expected response:
{
  "submission_id": "uuid",
  "status": "ready",
  "confidence_score": 0.92,
  "markers_count": 15,
  "priority_markers_found": 8,
  "needs_review": false,
  "markers": [...]
}
```

---

## Files in bloodwork_engine/

| File | Purpose |
|------|---------|
| `lab_upload.py` | FastAPI OCR upload endpoint |
| `junction_client.py` | Junction/Vital API client |
| `bloodwork_brain.py` | Brain handoff pipeline |
| `lab_integration_router.py` | Central router |
| `tests/test_lab_integration.py` | Test suite |
| `requirements_lab_integration.txt` | Dependencies |

---

## 8-Week Roadmap

| Week | Focus | Status |
|------|-------|--------|
| 1-2 | OCR pipeline | ✅ Complete |
| 3-4 | Junction API integration | ✅ Complete |
| 5-6 | Brain pipeline connection | ⏳ Pending |
| 7-8 | Production hardening | ⏳ Pending |

---

## Troubleshooting

### Migration Fails
- Check for existing tables: `DROP TABLE IF EXISTS` first
- Verify PostgreSQL version >= 13 (for gen_random_uuid())

### OCR Returns Low Confidence
- Check image quality (>200 DPI recommended)
- Verify PDF is text-based, not scanned
- Review biomarker aliases in `BIOMARKER_ALIASES` dict

### Railway Deployment Fails
- Check build logs in Railway dashboard
- Verify all imports are resolved
- Ensure ANTHROPIC_API_KEY is set

---

## Support

- **Railway:** https://railway.app/project/174e8e92-07e3-4c37-a67a-4f69b9098f78
- **GitHub:** https://github.com/hemichaeli/genomax2-api
- **Junction:** https://www.tryvital.io/labs
