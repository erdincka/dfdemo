"""FastAPI application entry point for the dfdemo web app."""

import logging
import sys
import time
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import os

from models.schemas import (
    ConnectionRequest,
    ConnectionStatus,
    DemoRunRequest,
    SetupRequest,
    CommandResult,
)
from services.ssh import ssh_service
from services.mapr_api import mapr_api
from services.demos import DEMO_REGISTRY

# ─── Production Logging Configuration ─────────────────────────────────────────

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
LOG_FORMAT = os.environ.get(
    "LOG_FORMAT",
    "%(asctime)s | %(levelname)-8s | %(name)s:%(lineno)d | %(message)s"
)

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format=LOG_FORMAT,
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("dfdemo")

# Suppress noisy third-party loggers
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("paramiko").setLevel(logging.WARNING)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    logger.info("=" * 60)
    logger.info("dfdemo backend starting up")
    logger.info("  Version: 1.0.0")
    logger.info("  Log level: %s", LOG_LEVEL)
    logger.info("  PID: %d", os.getpid())
    logger.info("=" * 60)
    yield
    # Cleanup
    logger.info("Shutting down: closing SSH and API connections")
    ssh_service.disconnect()
    mapr_api.close()
    logger.info("dfdemo backend stopped cleanly")


app = FastAPI(
    title="dfdemo - HPE Data Fabric Demo Runner",
    description="Web-based demo runner for HPE Data Fabric (MapR) clusters",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS middleware for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─── Request Logging Middleware ───────────────────────────────────────────────


@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Log all API requests with timing for monitoring."""
    start_time = time.time()
    response = await call_next(request)
    duration_ms = (time.time() - start_time) * 1000

    # Log API calls (skip static assets)
    if request.url.path.startswith("/api/"):
        logger.info(
            "API | %s %s | %d | %.1fms",
            request.method,
            request.url.path,
            response.status_code,
            duration_ms,
        )

    return response


# ─── Connection Endpoints ────────────────────────────────────────────────────


@app.post("/api/connect", response_model=ConnectionStatus)
async def connect(request: ConnectionRequest):
    """Test and establish SSH connection to the Data Fabric cluster."""
    logger.info(
        "CONNECT | Attempting connection to %s:%d as '%s'",
        request.hostname, request.port, request.username,
    )

    success, message = ssh_service.connect(
        hostname=request.hostname,
        username=request.username,
        password=request.password,
        port=request.port,
    )

    if success:
        # Configure the MapR API client
        mapr_api.configure(request.hostname, request.username, request.password)
        # Get cluster info
        cluster_info = ssh_service.get_cluster_info()
        logger.info(
            "CONNECT | Success | host=%s | cluster=%s",
            request.hostname,
            cluster_info.get("cluster_name", "unknown"),
        )
        return ConnectionStatus(
            success=True,
            message=message,
            cluster_info=cluster_info,
        )
    else:
        logger.warning("CONNECT | Failed | host=%s | reason=%s", request.hostname, message)
        return ConnectionStatus(success=False, message=message)


@app.post("/api/disconnect")
async def disconnect():
    """Disconnect from the cluster."""
    logger.info("DISCONNECT | Closing connections to %s", ssh_service.hostname or "N/A")
    ssh_service.disconnect()
    mapr_api.close()
    return {"success": True, "message": "Disconnected"}


@app.get("/api/connection/status")
async def connection_status():
    """Get current connection status."""
    return {
        "connected": ssh_service.is_connected,
        "hostname": ssh_service.hostname,
        "username": ssh_service.username,
    }


# ─── Demo Endpoints ──────────────────────────────────────────────────────────


@app.get("/api/demos")
async def list_demos():
    """List all available demos."""
    demos = []
    for demo_id, demo in DEMO_REGISTRY.items():
        info = demo["info"]()
        demos.append(info.model_dump())
    return {"demos": demos}


@app.get("/api/demos/{demo_id}/steps")
async def get_demo_steps(demo_id: str):
    """Get the steps for a specific demo."""
    if demo_id not in DEMO_REGISTRY:
        raise HTTPException(status_code=404, detail=f"Demo '{demo_id}' not found")

    steps = DEMO_REGISTRY[demo_id]["steps"]()
    return {"demo_id": demo_id, "steps": [s.model_dump() for s in steps]}


@app.get("/api/demos/{demo_id}/prerequisites")
async def check_prerequisites(demo_id: str):
    """Check prerequisites for a specific demo."""
    if demo_id not in DEMO_REGISTRY:
        raise HTTPException(status_code=404, detail=f"Demo '{demo_id}' not found")

    logger.info("PREREQ_CHECK | demo=%s", demo_id)
    prereqs = DEMO_REGISTRY[demo_id]["prerequisites"]()

    passed = sum(1 for p in prereqs if p.status == "pass")
    failed = sum(1 for p in prereqs if p.status == "fail")
    warned = sum(1 for p in prereqs if p.status == "warn")
    logger.info(
        "PREREQ_CHECK | demo=%s | total=%d | pass=%d | fail=%d | warn=%d",
        demo_id, len(prereqs), passed, failed, warned,
    )

    return {
        "demo_id": demo_id,
        "prerequisites": [p.model_dump() for p in prereqs],
        "all_passed": all(p.status == "pass" for p in prereqs),
    }


@app.post("/api/demos/{demo_id}/setup")
async def setup_prerequisite(demo_id: str, request: SetupRequest):
    """Run setup for a specific prerequisite."""
    if demo_id not in DEMO_REGISTRY:
        raise HTTPException(status_code=404, detail=f"Demo '{demo_id}' not found")

    logger.info("PREREQ_FIX | demo=%s | prereq=%s", demo_id, request.prerequisite_name)
    result = DEMO_REGISTRY[demo_id]["setup"](request.prerequisite_name)
    logger.info(
        "PREREQ_FIX | demo=%s | prereq=%s | success=%s",
        demo_id, request.prerequisite_name, result.success,
    )
    return result.model_dump()


@app.post("/api/demos/{demo_id}/setup-all")
async def setup_all_prerequisites(demo_id: str):
    """Run setup for all failing prerequisites."""
    if demo_id not in DEMO_REGISTRY:
        raise HTTPException(status_code=404, detail=f"Demo '{demo_id}' not found")

    logger.info("PREREQ_FIX_ALL | demo=%s", demo_id)
    results = []
    for result in DEMO_REGISTRY[demo_id]["setup_all"]():
        results.append(result.model_dump())

    logger.info(
        "PREREQ_FIX_ALL | demo=%s | fixed=%d | failed=%d",
        demo_id,
        sum(1 for r in results if r.get("success")),
        sum(1 for r in results if not r.get("success")),
    )
    return {"results": results}


@app.post("/api/demos/{demo_id}/run-step")
async def run_demo_step(demo_id: str, request: DemoRunRequest):
    """Execute a specific demo step."""
    if demo_id not in DEMO_REGISTRY:
        raise HTTPException(status_code=404, detail=f"Demo '{demo_id}' not found")

    logger.info("DEMO_STEP | demo=%s | step=%d", demo_id, request.step_id)
    result = DEMO_REGISTRY[demo_id]["run_step"](request.step_id, request.params)
    logger.info(
        "DEMO_STEP | demo=%s | step=%d | success=%s",
        demo_id, request.step_id, result.success,
    )
    return result.model_dump()


# ─── WebSocket for streaming command output ──────────────────────────────────


@app.websocket("/ws/execute")
async def websocket_execute(websocket: WebSocket):
    """WebSocket endpoint for streaming command execution output."""
    await websocket.accept()
    logger.info("WS | Client connected")

    try:
        while True:
            data = await websocket.receive_json()
            command = data.get("command", "")

            if not command:
                await websocket.send_json({"error": "No command provided"})
                continue

            logger.info("WS | Executing: %s", command[:100])
            await websocket.send_json({"type": "start", "command": command})

            for chunk in ssh_service.execute_streaming(command):
                await websocket.send_json({"type": "output", "data": chunk})

            await websocket.send_json({"type": "end"})

    except WebSocketDisconnect:
        logger.info("WS | Client disconnected")


# ─── Health check ─────────────────────────────────────────────────────────────


@app.get("/api/health")
async def health():
    """Health check endpoint for Kubernetes probes."""
    return {
        "status": "healthy",
        "version": "1.0.0",
        "connected": ssh_service.is_connected,
        "cluster": ssh_service.hostname if ssh_service.is_connected else None,
    }


# ─── Static file serving (production) ────────────────────────────────────────

static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "frontend", "dist")
if os.path.isdir(static_dir):
    from fastapi.responses import FileResponse

    @app.get("/")
    async def serve_index():
        return FileResponse(os.path.join(static_dir, "index.html"))

    app.mount("/assets", StaticFiles(directory=os.path.join(static_dir, "assets")), name="assets")
    logger.info("Frontend served from %s", static_dir)
else:
    logger.warning("Frontend dist not found at %s - running in API-only mode", static_dir)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)