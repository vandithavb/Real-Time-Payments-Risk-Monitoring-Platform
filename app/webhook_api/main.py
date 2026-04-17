from fastapi import FastAPI
from app.common.config import APP_ENV
from app.webhook_api.stripe_handler import router as stripe_router

app = FastAPI(title="Real-Time Payments Risk Monitoring Platform")
app.include_router(stripe_router)

@app.get("/health")
def health() -> dict:
    return {
        "status": "ok",
        "environment": APP_ENV
    }