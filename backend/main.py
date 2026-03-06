"""Main FastAPI application for the Alternance Dashboard."""

import sys
import os
import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

# Add backend directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database import init_db
from api.routes import router

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="FUA - Find Ur Alternance",
    description="Agrégateur d'offres d'alternance en France",
    version="1.0.0",
)

# CORS middleware (allow frontend to call API)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routes
app.include_router(router)

# Serve static frontend files
frontend_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "frontend")
if os.path.exists(frontend_dir):
    app.mount("/", StaticFiles(directory=frontend_dir, html=True), name="frontend")


@app.on_event("startup")
async def startup():
    """Initialize the database and scheduler on startup."""
    logger.info("Initializing database...")
    init_db()
    logger.info("Database initialized successfully.")

    # Setup Scheduler for automatic scraping
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    from api.routes import run_global_scrape
    
    scheduler = AsyncIOScheduler()
    # Schedule 3 times a day (morning, afternoon, evening) with random jitter
    # This ensures tools like LinkedIn are less likely to hit rate limits while keeping data fresh.
    scheduler.add_job(run_global_scrape, 'cron', hour='8,14,20', minute=0, jitter=7200)
    scheduler.start()
    logger.info("Background scheduler started: Scraping scheduled 3 times per day (with random jitter).")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=3080, reload=True)
