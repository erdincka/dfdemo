"""FastAPI application entry point for the dfdemo web app."""

import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s:%(lineno)d - %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    logger.info("Starting dfdemo backend...")
    yield
    # Cleanup
    ssh_service.disconnect()
    mapr_api.close()
    logger.info("dfdemo backend stopped.")


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


# ─── Connection Endpoints ────────────────────────────────────────────────────


@app.post("/api/connect", response_model=ConnectionStatus)
async def connect(request: ConnectionRequest):
    """Test and establish SSH connection to the Data Fabric cluster."""
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
        return ConnectionStatus(
            success=True,
            message=message,
            cluster_info=cluster_info,
        )
    else:
        return ConnectionStatus(success=False, message=message)


@app.post("/api/disconnect")
async def disconnect():
    """Disconnect from the cluster."""
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

    prereqs = DEMO_REGISTRY[demo_id]["prerequisites"]()
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

    result = DEMO_REGISTRY[demo_id]["setup"](request.prerequisite_name)
    return result.model_dump()


@app.post("/api/demos/{demo_id}/setup-all")
async def setup_all_prerequisites(demo_id: str):
    """Run setup for all failing prerequisites."""
    if demo_id not in DEMO_REGISTRY:
        raise HTTPException(status_code=404, detail=f"Demo '{demo_id}' not found")

    results = []
    for result in DEMO_REGISTRY[demo_id]["setup_all"]():
        results.append(result.model_dump())

    return {"results": results}


@app.post("/api/demos/{demo_id}/run-step")
async def run_demo_step(demo_id: str, request: DemoRunRequest):
    """Execute a specific demo step."""
    if demo_id not in DEMO_REGISTRY:
        raise HTTPException(status_code=404, detail=f"Demo '{demo_id}' not found")

    result = DEMO_REGISTRY[demo_id]["run_step"](request.step_id, request.params)
    return result.model_dump()


# ─── WebSocket for streaming command output ──────────────────────────────────


@app.websocket("/ws/execute")
async def websocket_execute(websocket: WebSocket):
    """WebSocket endpoint for streaming command execution output."""
    await websocket.accept()

    try:
        while True:
            data = await websocket.receive_json()
            command = data.get("command", "")

            if not command:
                await websocket.send_json({"error": "No command provided"})
                continue

            # Stream output back
            await websocket.send_json({"type": "start", "command": command})

            for chunk in ssh_service.execute_streaming(command):
                await websocket.send_json({"type": "output", "data": chunk})

            await websocket.send_json({"type": "end"})

    except WebSocketDisconnect:
        logger.info("WebSocket client disconnected")


# ─── Health check ─────────────────────────────────────────────────────────────


@app.get("/api/health")
async def health():
    """Health check endpoint."""
    return {"status": "healthy", "connected": ssh_service.is_connected}


# Serve frontend static files (for production)
# Note: Mounted at /app to avoid conflicts with API routes
static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "frontend", "dist")
if os.path.isdir(static_dir):
    # Serve index.html at root for SPA routing
    from fastapi.responses import FileResponse

    @app.get("/")
    async def serve_index():
        return FileResponse(os.path.join(static_dir, "index.html"))

    app.mount("/assets", StaticFiles(directory=os.path.join(static_dir, "assets")), name="assets")
else:
    logger.warning("Frontend dist not found at %s - API only mode", static_dir)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="[IP_ADDRESS]", port=8000)