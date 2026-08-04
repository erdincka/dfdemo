# ─── Stage 1: Build frontend ─────────────────────────────────────────────────
FROM node:20-alpine AS frontend-build

WORKDIR /app/frontend
COPY frontend/package.json frontend/package-lock.json* ./
RUN npm ci --silent
COPY frontend/ ./
RUN npm run build

# ─── Stage 2: Backend + runtime ──────────────────────────────────────────────
FROM python:3.12-slim AS runtime

LABEL org.opencontainers.image.ref.name="dfdemo"
LABEL org.opencontainers.image.description="HPE Data Fabric Demo Runner - Security & Governance"
LABEL org.opencontainers.image.authors="erdincka"
LABEL org.opencontainers.image.version="1.0.0"

# Install system dependencies for NFS mounting and SSH
RUN apt-get update && apt-get install -y --no-install-recommends \
    nfs-common \
    openssh-client \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

WORKDIR /app

# Install Python dependencies (pinned, no cache)
COPY backend/requirements.txt .
RUN pip install --no-cache-dir --disable-pip-version-check -r requirements.txt \
    && rm -rf /root/.cache

# Copy backend code
COPY backend/ ./

# Copy built frontend from stage 1
COPY --from=frontend-build /app/frontend/dist ./frontend/dist

# Create mount point for NFS volumes
RUN mkdir -p /mapr

# Security: run as non-root user
RUN useradd --create-home --shell /bin/bash appuser \
    && chown -R appuser:appuser /app
USER appuser

# Environment defaults
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    LOG_LEVEL=INFO

# Expose port
EXPOSE 8000

# Health check for Kubernetes/container orchestration
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD curl -sf http://localhost:8000/api/health || exit 1

# Production entrypoint
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1", "--log-level", "warning", "--access-log"]