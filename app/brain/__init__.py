powershell -Command "Set-Content -Path 'app\brain\__init__.py' -Value 'from fastapi import APIRouter

brain_router = APIRouter(prefix=\"/api/v1/brain\", tags=[\"Brain\"])

@brain_router.get(\"/health\")
async def brain_health():
    return {\"status\": \"healthy\", \"service\": \"brain\", \"version\": \"brain_1.0.0\"}

@brain_router.get(\"/info\")
async def brain_info():
    return {\"name\": \"GenoMAX2 Brain\", \"version\": \"brain_1.0.0\"}' -Encoding UTF8"