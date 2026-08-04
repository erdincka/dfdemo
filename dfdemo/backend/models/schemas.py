"""Pydantic models for request/response schemas."""

from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field


class ConnectionRequest(BaseModel):
    """Request to connect to a Data Fabric cluster."""
    hostname: str = Field(..., description="Cluster hostname or IP")
    username: str = Field(..., description="SSH username")
    password: str = Field(..., description="SSH password")
    port: int = Field(default=22, description="SSH port")


class ConnectionStatus(BaseModel):
    """Response for connection test."""
    success: bool
    message: str
    cluster_info: Optional[dict] = None


class PrerequisiteStatus(str, Enum):
    """Status of a prerequisite check."""
    PASS = "pass"
    FAIL = "fail"
    WARN = "warn"
    UNKNOWN = "unknown"


class Prerequisite(BaseModel):
    """A single prerequisite check result."""
    name: str
    description: str
    status: PrerequisiteStatus
    message: str = ""
    fix_command: Optional[str] = None


class DemoInfo(BaseModel):
    """Information about an available demo."""
    id: str
    name: str
    description: str
    keywords: list[str] = []


class DemoStep(BaseModel):
    """A single step in a demo."""
    id: int
    title: str
    description: str
    command: Optional[str] = None
    api_call: Optional[str] = None
    expected_result: str = ""


class CommandResult(BaseModel):
    """Result of a command execution."""
    command: str
    stdout: str = ""
    stderr: str = ""
    exit_code: int = 0
    success: bool = True


class DemoRunRequest(BaseModel):
    """Request to run a demo step."""
    demo_id: str
    step_id: int
    params: dict = {}


class SetupRequest(BaseModel):
    """Request to run prerequisite setup."""
    demo_id: str
    prerequisite_name: str