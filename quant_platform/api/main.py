# -*- coding: utf-8 -*-
"""
Control Console API - Main FastAPI Application
"""

import os
import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan events"""
    logger.info("Starting Quant Platform Control Console...")

    # Initialize connections
    # - OSS
    # - Redis (optional)
    # - Database (optional)

    yield

    logger.info("Shutting down Quant Platform Control Console...")


# Create FastAPI app
app = FastAPI(
    title="Quant Platform Control Console",
    description="Distributed Backtest Scheduler API",
    version="1.0.0",
    lifespan=lifespan,
    docs_url=None,
    redoc_url=None,
)


# Custom Swagger UI with domestic CDN
@app.get("/docs", include_in_schema=False)
async def custom_docs():
    return HTMLResponse("""<!DOCTYPE html>
<html>
<head>
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <link type="text/css" rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5.18.2/swagger-ui.css">
    <link rel="shortcut icon" type="image/png" href="https://unpkg.com/swagger-ui-dist@5.18.2/favicon-32x32.png">
    <title>Quant Platform Control Console - Swagger UI</title>
</head>
<body>
    <div id="swagger-ui"></div>
    <script src="https://unpkg.com/swagger-ui-dist@5.18.2/swagger-ui-bundle.js"></script>
    <script>
        window.onload = function() {
            const ui = SwaggerUIBundle({
                url: '/openapi.json',
                dom_id: "#swagger-ui",
                deepLinking: true,
                presets: [
                    SwaggerUIBundle.presets.apis
                ],
                layout: "BaseLayout"
            });
            window.ui = ui;
        };
    </script>
</body>
</html>""")


# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.exception(f"Unhandled exception: {exc}")
    return JSONResponse(
        status_code=500,
        content={"detail": str(exc)}
    )


# Import and include routers
from .backtest import router as backtest_router
from .result import router as result_router

app.include_router(backtest_router)
app.include_router(result_router)


# Health check
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "quant-platform-control-console"
    }


# API info endpoint
@app.get("/api")
async def api_info():
    """API info endpoint"""
    return {
        "name": "Quant Platform Control Console",
        "version": "1.0.0",
        "endpoints": {
            "backtest": "/api/backtest",
            "result": "/api/result",
            "docs": "/docs",
            "openapi": "/openapi.json"
        }
    }


# Web UI redirect to docs
@app.get("/", response_class=HTMLResponse)
async def web_ui():
    return HTMLResponse(content="""
        <html>
            <head><title>Quant Platform</title></head>
            <body>
                <h1>Quant Platform Control Console</h1>
                <p><a href="/docs">API Documentation</a></p>
            </body>
        </html>
    """)


def run_server():
    """Run the API server"""
    import uvicorn

    host = os.getenv("API_HOST", "0.0.0.0")
    port = int(os.getenv("API_PORT", "8000"))

    uvicorn.run(
        "quant_platform.api.main:app",
        host=host,
        port=port,
        reload=False,
        log_level="info"
    )


if __name__ == "__main__":
    run_server()
