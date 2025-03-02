from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import os
import uvicorn
from pathlib import Path

from app.api.endpoints import users, products, purchases, social, benchmark
from app.core.config import settings
from app.db.init_db import init_db

app = FastAPI(
    title="Social Network Purchase Analysis API",
    description="API for analyzing user purchasing behavior in a social network",
    version="0.1.0",
)

# Set up CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(users.router, prefix="/users", tags=["users"])
app.include_router(products.router, prefix="/products", tags=["products"])
app.include_router(purchases.router, tags=["purchases"])
app.include_router(social.router, tags=["social"])
app.include_router(benchmark.router, prefix="/benchmark", tags=["benchmark"])

# Create static directory if it doesn't exist
static_dir = Path(__file__).parent / "static"
static_dir.mkdir(exist_ok=True)

# Mount static files
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

@app.on_event("startup")
async def startup_event():
    await init_db()

@app.get("/")
async def root():
    return {
        "message": "Welcome to the Social Network Purchase Analysis API",
        "docs": "/docs",
        "benchmark": "/static/index.html",
    }

if __name__ == "__main__":
    # Use port 8005 to match the frontend
    port = int(os.environ.get("PORT", 8005))
    uvicorn.run("app.main:app", host="0.0.0.0", port=port, reload=True) 