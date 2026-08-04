# ─── Stage 1: Build frontend ─────────────────────────────────────────────────
FROM node:20-alpine AS frontend-build

WORKDIR /app/frontend
COPY frontend/package.json frontend/package-lock.json* ./
RUN npm install
COPY frontend/ ./
RUN npm run build

# ─── Stage 2: Backend + runtime ──────────────────────────────────────────────
FROM python:3.12-slim AS runtime

LABEL org.opencontainers.image.ref.name="dfdemo"
LABEL org.opencontainers.image.description="HPE Data Fabric Demo Runner"
LABEL org.opencontainers.image.authors="erdincka"

# Install system dependencies for NFS mounting and SSH
RUN apt-get update && apt-get install -y --no-install-recommends \
    nfs-common \
    openssh-client \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies
COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy backend code
COPY backend/ ./

# Copy built frontend
COPY --from=frontend-build /app/frontend/dist ./frontend/dist

# Create mount point for NFS
RUN mkdir -p /mapr

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8000/api/health || exit 1

# Run the app
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]