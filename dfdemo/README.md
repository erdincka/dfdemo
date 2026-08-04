# dfdemo - HPE Data Fabric Demo Runner

A containerized web application for running guided demos against an HPE Data Fabric (MapR) cluster.

## Features

- **SSH Connectivity Check** — Connect to any Data Fabric cluster with username/password authentication
- **Prerequisite Validation** — Automatically checks if required users, volumes, tables, and groups exist
- **Automated Setup** — Creates missing artifacts on demand (with sudo privileges)
- **Guided Demo Execution** — Step-by-step demo with clear command/API output display
- **NFS Mount Capable** — Container runs in privileged mode with `SYS_ADMIN` for NFS volume mounts

## Included Demo: Security & Governance

Demonstrates two key aspects of HPE Data Fabric security:

### 1. Dynamic Data Masking (DDM)
- Creates a JSON document table with PII fields (email, SSN, birthdate, credit card)
- Applies masking rules (`mrddm_email`, `mrddm_ssn`, `mrddm_date`, `mrddm_last4`)
- Shows data as **admin** (unmasked) vs **restricted user** (masked)

### 2. Policy-Based Access Control (ACE)
- Creates a volume with specific Access Control Expressions
- `demo_admin` has read+write access
- `demo_analyst` has read-only access (via group membership)
- Demonstrates write denial for restricted user

## Quick Start

### Build & Run

```bash
cd dfdemo
docker compose up -d --build
```

The app will be available at **http://localhost:8000**

### Manual Docker Build

```bash
docker build -t local/dfdemo:1.0.0 .
docker run -d --privileged --cap-add SYS_ADMIN -p 8000:8000 local/dfdemo:1.0.0
```

### Development Mode

**Backend:**
```bash
cd backend
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```

**Frontend:**
```bash
cd frontend
npm install
npm run dev
```

The frontend dev server (port 3000) proxies API calls to the backend (port 8000).

## Architecture

```
dfdemo/
├── Dockerfile              # Multi-stage build (Node + Python)
├── docker-compose.yaml     # Privileged container with SYS_ADMIN
├── backend/
│   ├── main.py             # FastAPI app with REST + WebSocket endpoints
│   ├── models/
│   │   └── schemas.py      # Pydantic request/response models
│   └── services/
│       ├── ssh.py          # SSH session management (paramiko)
│       ├── mapr_api.py     # Data Fabric REST API client (ports 8443/8243)
│       └── demos.py        # Demo definitions, prerequisites, step execution
└── frontend/
    └── src/
        ├── App.tsx          # Main app with view routing
        ├── api.ts           # API client
        └── components/
            ├── ConnectionForm.tsx    # Cluster connection
            ├── DemoSelector.tsx      # Demo selection
            ├── PrerequisiteCheck.tsx # Prerequisite validation & fix
            └── DemoRunner.tsx        # Step-by-step demo execution
```

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/connect` | Test SSH connection |
| POST | `/api/disconnect` | Disconnect from cluster |
| GET | `/api/connection/status` | Current connection status |
| GET | `/api/demos` | List available demos |
| GET | `/api/demos/{id}/steps` | Get demo steps |
| GET | `/api/demos/{id}/prerequisites` | Check prerequisites |
| POST | `/api/demos/{id}/setup` | Fix a specific prerequisite |
| POST | `/api/demos/{id}/setup-all` | Fix all failing prerequisites |
| POST | `/api/demos/{id}/run-step` | Execute a demo step |
| GET | `/api/health` | Health check |
| WS | `/ws/execute` | Streaming command execution |

## Requirements

- Docker with privileged mode support
- Network access to the Data Fabric cluster (SSH port 22, REST API ports 8443/8243)
- Cluster credentials with sufficient privileges to create users, volumes, and tables
- Sudo access recommended for automated user/group creation

## Data Fabric Documentation

- [HPE Ezmeral Data Fabric 8.1](https://docs.ezmeral.hpe.com/datafabric-customer-managed/81/index.html)
- [Dynamic Data Masking](https://docs.ezmeral.hpe.com/datafabric-customer-managed/81/SecurityGuide/DDM.html)
- [Access Control Expressions](https://docs.ezmeral.hpe.com/datafabric-customer-managed/81/SecurityGuide/ACE.html)