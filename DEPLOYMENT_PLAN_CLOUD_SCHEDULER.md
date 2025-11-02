# BemiDB Production Deployment Plan (Cloud Scheduler + Fly.io)

**Infrastructure Stack:**
- **Server**: Fly.io (Postgres wire protocol, always-on)
- **Catalog**: Neon PostgreSQL (managed Postgres)
- **Storage**: Cloudflare R2 (S3-compatible)
- **Cron**: Google Cloud Scheduler (calls Fly.io Machines API)

**Code Modifications:** DeleteOldTables() removed to support independent table syncing on different schedules.

**Total Monthly Cost:** ~$35-55/month (vs $300-1,000 for Snowflake + Fivetran)

---

## Table of Contents

1. [Repository Setup](#repository-setup)
2. [Architecture Overview](#architecture-overview)
3. [Prerequisites](#prerequisites)
4. [Build Custom Docker Image](#build-custom-docker-image)
5. [Infrastructure Setup](#infrastructure-setup)
6. [Google Cloud Scheduler Setup](#google-cloud-scheduler-setup)
7. [Testing & Verification](#testing--verification)
8. [Multiple Sync Schedules](#multiple-sync-schedules)
9. [Monitoring & Logging](#monitoring--logging)
10. [Cost Breakdown](#cost-breakdown)
11. [Troubleshooting](#troubleshooting)
12. [Maintenance](#maintenance)

---

## Repository Setup

### Why Two Repositories?

This deployment requires **two separate repositories** for clean separation of concerns:

1. **BemiDB Fork** (your customized version)
   - Contains code modifications (DeleteOldTables removed)
   - Builds custom Docker image
   - Pushed to GitHub Container Registry (ghcr.io)

2. **BemiDB Deployment** (infrastructure as code)
   - Contains Fly.io configuration files
   - Cloud Scheduler setup scripts
   - Environment variable templates
   - Deployment documentation

**Architecture:**
```
┌─────────────────────────────────────┐
│  Repository 1: BemiDB Fork          │
│  (Your GitHub account)              │
│                                      │
│  - Modified code (DeleteOldTables)  │
│  - Dockerfile                       │
│  - GitHub Actions for CI/CD         │
│                                      │
│  Builds → ghcr.io/YOUR-ORG/bemidb   │
└─────────────────────────────────────┘
                  ↓
          Docker Image Published
                  ↓
┌─────────────────────────────────────┐
│  Repository 2: BemiDB Deployment    │
│  (Your GitHub account)              │
│                                      │
│  - fly.toml (server config)         │
│  - fly.syncer.toml (syncer config)  │
│  - scripts/ (deployment helpers)    │
│  - .env.sample (config template)    │
│                                      │
│  Uses → ghcr.io/YOUR-ORG/bemidb     │
└─────────────────────────────────────┘
```

---

### Step 1: Fork BemiDB Repository

**Duration:** 5 minutes

#### 1.1 Fork on GitHub

```bash
# Go to https://github.com/BemiHQ/BemiDB
# Click "Fork" button (top right)
# Select your account/organization
# Fork name: "BemiDB" (or "bemidb-custom")

# Clone your fork
git clone https://github.com/YOUR-USERNAME/BemiDB.git
cd BemiDB

# Add upstream remote (for future updates)
git remote add upstream https://github.com/BemiHQ/BemiDB.git

# Verify code modification is present
grep -A 5 "DeleteOldTables" src/syncer-postgres/lib/syncer_full_refresh.go

# Expected output showing commented code:
# // NOTE: DeleteOldTables() has been removed...
# // syncer.Utils.DeleteOldTables(icebergTableNames)
```

#### 1.2 Create GitHub Container Registry

```bash
# Enable GitHub Container Registry for your account/org
# 1. Go to https://github.com/settings/packages
# 2. Click "Enable improved container support"

# Create Personal Access Token (PAT) for pushing images
# 1. Go to https://github.com/settings/tokens
# 2. Click "Generate new token (classic)"
# 3. Name: "BemiDB Docker Push"
# 4. Scopes: Select "write:packages", "read:packages"
# 5. Click "Generate token"
# 6. Save token: ghp_abc123...

# Login to GitHub Container Registry
export CR_PAT="ghp_abc123..."  # Your token
echo $CR_PAT | docker login ghcr.io -u YOUR-USERNAME --password-stdin

# Verify login
# Login Succeeded
```

#### 1.3 Tag Your Fork

```bash
# Create a version tag for your custom build
git tag -a v1.7.0-custom.1 -m "BemiDB v1.7.0 with DeleteOldTables removed"
git push origin v1.7.0-custom.1

# List tags
git tag
# v1.7.0-custom.1
```

---

### Step 2: Create Deployment Repository

**Duration:** 5 minutes

#### 2.1 Create New Repository

```bash
# On GitHub:
# 1. Go to https://github.com/new
# 2. Repository name: "bemidb-deployment"
# 3. Description: "BemiDB production deployment configuration"
# 4. Private or Public: Your choice (Private recommended for secrets)
# 5. Initialize with README: Yes
# 6. Click "Create repository"

# Clone deployment repository
cd ..  # Exit BemiDB fork
git clone https://github.com/YOUR-USERNAME/bemidb-deployment.git
cd bemidb-deployment
```

#### 2.2 Create Directory Structure

```bash
# Create directory structure
mkdir -p config scripts docs

# Create README
cat > README.md << 'EOF'
# BemiDB Production Deployment

This repository contains infrastructure-as-code for deploying BemiDB to production.

## Components

- **Fly.io Server**: Postgres wire protocol (always-on)
- **Fly.io Syncer**: On-demand sync machine (pay-per-second)
- **Google Cloud Scheduler**: Cron jobs for automated syncing
- **Neon PostgreSQL**: Catalog database
- **Cloudflare R2**: Object storage for Parquet files

## Quick Start

See [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) for complete setup instructions.

## Repository Structure

```
.
├── config/
│   ├── fly.toml              # Server configuration
│   ├── fly.syncer.toml       # Syncer configuration
│   └── .env.sample           # Environment variables template
├── scripts/
│   ├── setup-cloud-scheduler.sh   # Create Cloud Scheduler jobs
│   ├── deploy-server.sh           # Deploy BemiDB server
│   └── deploy-syncer.sh           # Deploy syncer machine
├── docs/
│   └── DEPLOYMENT.md         # Full deployment guide
└── README.md
```

## Documentation

- [Full Deployment Guide](docs/DEPLOYMENT.md)
- [Cost Analysis](docs/COST.md)
- [Troubleshooting](docs/TROUBLESHOOTING.md)

## Support

File issues at: https://github.com/YOUR-USERNAME/bemidb-deployment/issues
EOF

# Commit README
git add README.md
git commit -m "Initial commit: project structure"
git push origin main
```

#### 2.3 Copy Configuration Files

```bash
# We'll create these files in the next steps
# For now, create placeholders

cat > config/.env.sample << 'EOF'
# Source Database
SOURCE_POSTGRES_DATABASE_URL=postgresql://user:password@host:5432/database

# Catalog Database (Neon)
CATALOG_DATABASE_URL=postgresql://user:password@ep-xxx.neon.tech/catalog?sslmode=require

# S3-Compatible Storage (Cloudflare R2)
AWS_REGION=auto
AWS_S3_BUCKET=bemidb-data
AWS_ACCESS_KEY_ID=your-r2-access-key
AWS_SECRET_ACCESS_KEY=your-r2-secret-key
AWS_S3_ENDPOINT=https://account-id.r2.cloudflarestorage.com

# Fly.io Configuration
FLY_API_TOKEN=FlyV1_your-token-here
FLY_MACHINE_ID=your-machine-id-here

# Google Cloud
GCP_PROJECT_ID=your-gcp-project
GCP_REGION=us-east1
EOF

git add config/.env.sample
git commit -m "Add environment variables template"
git push origin main
```

---

## Architecture Overview

### How It Works

```
┌─────────────────────────────────────────────┐
│   Google Cloud Scheduler                    │
│   Reliable cron service with 99.9% SLA      │
│   $0.10/job/month per schedule              │
│                                              │
│   Schedule 1: Every hour      ──────────────┼──┐
│   Schedule 2: Every 6 hours   ──────────────┼──┤
│   Schedule 3: Daily at 2 AM   ──────────────┼──┤
└─────────────────────────────────────────────┘  │
                                                  │
         HTTP POST to Fly Machines API            │
         with table list in request body          │
                                                  │
                                                  ↓
┌──────────────────────────────────────────────────────────┐
│   Fly.io Machine: bemidb-syncer                          │
│   (Stopped by default, pay per second)                   │
│                                                           │
│   1. Receives API call with env vars                     │
│   2. Starts machine (2-5 seconds)                        │
│   3. Reads SOURCE_POSTGRES_INCLUDE_TABLES from env       │
│   4. Syncs specified tables                              │
│   5. Writes Parquet files to R2                          │
│   6. Updates Neon catalog                                │
│   7. Exits (machine auto-stops)                          │
│                                                           │
│   Cost: ~$0.0003/second (only while running)             │
└──────────────────────────────────────────────────────────┘
         │                                    │
         ↓                                    ↓
┌──────────────────┐              ┌─────────────────────┐
│  Neon PostgreSQL │              │  Cloudflare R2      │
│  (Catalog)       │              │  (Parquet Storage)  │
│  ~$5/month       │              │  ~$1.50/month       │
└──────────────────┘              └─────────────────────┘
         ↑                                    ↑
         │                                    │
         └──────────────┬─────────────────────┘
                        │
                        │ Queries catalog + reads Parquet files
                        │
            ┌───────────▼──────────┐
            │  Fly.io Server       │
            │  bemidb-server       │
            │  (Always running)    │
            │  Port: 5432          │
            │  ~$30/month          │
            └──────────────────────┘
                        ↑
                        │ Postgres wire protocol
                        │
            ┌───────────┴──────────┐
            │   Client Apps        │
            │   BI Tools, psql     │
            │   Metabase, etc.     │
            └──────────────────────┘
```

### Key Advantages

**vs GitHub Actions:**
- ✅ **Guaranteed timing** - Cloud Scheduler has 99.9% SLA, no delays
- ✅ **Parameterized jobs** - Each schedule passes different table lists via API
- ✅ **Better monitoring** - Cloud Logging integrated
- ✅ **Enterprise-grade** - Suitable for production

**vs Always-On Cron Services:**
- ✅ **Pay per second** - Only charged when syncing (not idle time)
- ✅ **Cost-effective** - $5-25/month vs $30-50/month for always-on
- ✅ **Same infrastructure** - Syncer can run in same Fly.io region as server

**vs Manual Triggers:**
- ✅ **Automated** - No manual intervention needed
- ✅ **Reliable** - Automatic retries on transient failures
- ✅ **Scheduled** - Runs at exact times (no delays)

---

## Prerequisites

### Required Accounts

- [ ] **Fly.io account** - https://fly.io/signup
  - Credit card required (free credits available)
  - Will host BemiDB server and syncer

- [ ] **Neon account** - https://neon.tech
  - Free tier available
  - PostgreSQL catalog database

- [ ] **Cloudflare account** - https://dash.cloudflare.com
  - R2 storage for Parquet files
  - Free egress (unlike AWS S3)

- [ ] **Google Cloud account** - https://console.cloud.google.com
  - Cloud Scheduler for cron jobs
  - Free tier: $300 credit for 90 days

### Required Tools

```bash
# Install Fly.io CLI
curl -L https://fly.io/install.sh | sh

# Verify installation
flyctl version

# Install Google Cloud CLI
curl https://sdk.cloud.google.com | bash
exec -l $SHELL  # Restart shell

# Verify installation
gcloud --version

# Login to both services
flyctl auth login
gcloud auth login
```

### Source Database Requirements

Your source PostgreSQL database must:
- [ ] Be accessible via public internet (or use Tailscale/VPN)
- [ ] Allow connections from Fly.io IP ranges (or all IPs)
- [ ] Have SSL enabled (recommended): `?sslmode=require`
- [ ] Have a read-only user for syncing (recommended)

**Create read-only user:**
```sql
-- Connect to your source database
psql postgresql://admin@your-db.example.com:5432/production

-- Create read-only user
CREATE USER bemidb_syncer WITH PASSWORD 'secure-random-password';
GRANT CONNECT ON DATABASE production TO bemidb_syncer;
GRANT USAGE ON SCHEMA public TO bemidb_syncer;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO bemidb_syncer;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO bemidb_syncer;

-- Test connection
psql postgresql://bemidb_syncer:secure-random-password@your-db.example.com:5432/production -c "SELECT 1"
```

---

## Build Custom Docker Image

### Why Build a Custom Image?

The official BemiDB image (`ghcr.io/bemihq/bemidb:latest`) includes the `DeleteOldTables()` call, which deletes tables not in the current sync. Your fork has this removed, enabling independent table syncing.

**You need to build and publish your own Docker image** from your fork.

---

### Step 1: Set Up GitHub Actions for Auto-Build

**Duration:** 10 minutes

#### 1.1 Create GitHub Actions Workflow

```bash
# In your BemiDB fork repository
cd /path/to/BemiDB  # Your fork

# Create GitHub Actions workflow directory
mkdir -p .github/workflows

# Create Docker build workflow
cat > .github/workflows/docker-build.yml << 'EOF'
name: Build and Push Docker Image

on:
  push:
    branches:
      - main
    tags:
      - 'v*.*.*-custom.*'
  workflow_dispatch:

env:
  REGISTRY: ghcr.io
  IMAGE_NAME: ${{ github.repository }}

jobs:
  build-and-push:
    runs-on: ubuntu-latest
    permissions:
      contents: read
      packages: write

    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Log in to GitHub Container Registry
        uses: docker/login-action@v3
        with:
          registry: ${{ env.REGISTRY }}
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}

      - name: Extract metadata (tags, labels)
        id: meta
        uses: docker/metadata-action@v5
        with:
          images: ${{ env.REGISTRY }}/${{ env.IMAGE_NAME }}
          tags: |
            type=ref,event=branch
            type=ref,event=tag
            type=semver,pattern={{version}}
            type=semver,pattern={{major}}.{{minor}}
            type=raw,value=latest,enable={{is_default_branch}}

      - name: Build and push Docker image
        uses: docker/build-push-action@v5
        with:
          context: .
          push: true
          tags: ${{ steps.meta.outputs.tags }}
          labels: ${{ steps.meta.outputs.labels }}
          build-args: |
            PLATFORM=linux/amd64
EOF

# Commit and push workflow
git add .github/workflows/docker-build.yml
git commit -m "Add GitHub Actions workflow for Docker image build"
git push origin main
```

#### 1.2 Trigger Build

```bash
# The workflow will automatically trigger on push to main
# Check Actions tab on GitHub to see progress

# Or trigger manually via GitHub UI:
# 1. Go to Actions tab
# 2. Select "Build and Push Docker Image"
# 3. Click "Run workflow"
# 4. Select branch: main
# 5. Click "Run workflow"

# Monitor build progress
# https://github.com/YOUR-USERNAME/BemiDB/actions
```

#### 1.3 Verify Image Published

```bash
# After build completes (~5-10 minutes):
# 1. Go to your repo → Packages
# 2. You should see "bemidb" package
# 3. Copy the image URL: ghcr.io/YOUR-USERNAME/bemidb:latest

# Pull image to verify
docker pull ghcr.io/YOUR-USERNAME/bemidb:latest

# Test image locally
docker run --rm ghcr.io/YOUR-USERNAME/bemidb:latest syncer-postgres --help
```

---

### Step 2: Make Image Public (Optional but Recommended)

**Duration:** 2 minutes

```bash
# Via GitHub UI:
# 1. Go to https://github.com/YOUR-USERNAME?tab=packages
# 2. Click on "bemidb" package
# 3. Click "Package settings" (right sidebar)
# 4. Scroll to "Danger Zone"
# 5. Click "Change visibility"
# 6. Select "Public"
# 7. Type package name to confirm
# 8. Click "I understand, change package visibility"

# Why make it public?
# - No authentication needed to pull (simpler deployment)
# - Can be used by Fly.io, Cloud Run, etc. without registry auth
# - Still in your namespace (ghcr.io/YOUR-USERNAME/bemidb)
```

---

### Step 3: Update Deployment Repository

**Duration:** 5 minutes

#### 3.1 Create Fly Configuration Files

```bash
# Switch to deployment repository
cd /path/to/bemidb-deployment

# Create server configuration
cat > config/fly.toml << 'EOF'
app = "bemidb-server"
primary_region = "iad"  # us-east-1

[build]
  image = "ghcr.io/YOUR-USERNAME/bemidb:latest"  # ← Your custom image

[env]
  BEMIDB_PORT = "54321"
  BEMIDB_HOST = "0.0.0.0"
  BEMIDB_DATABASE = "bemidb"
  BEMIDB_LOG_LEVEL = "INFO"
  BEMIDB_DISABLE_ANONYMOUS_ANALYTICS = "true"
  AWS_REGION = "auto"

[[services]]
  internal_port = 54321
  protocol = "tcp"
  auto_stop_machines = false
  auto_start_machines = true
  min_machines_running = 1

  [[services.ports]]
    port = 5432
    handlers = ["tls"]

  [[services.ports]]
    port = 54321
    handlers = []

[[vm]]
  size = "shared-cpu-2x"
  memory = "4gb"

[http_service]
  internal_port = 54321
  force_https = false
  auto_stop_machines = false
  auto_start_machines = true
  min_machines_running = 1
EOF

# Create syncer configuration
cat > config/fly.syncer.toml << 'EOF'
app = "bemidb-syncer"
primary_region = "iad"

[build]
  image = "ghcr.io/YOUR-USERNAME/bemidb:latest"  # ← Your custom image

[env]
  DESTINATION_SCHEMA_NAME = "postgres"
  AWS_REGION = "auto"
  BEMIDB_LOG_LEVEL = "INFO"
  BEMIDB_DISABLE_ANONYMOUS_ANALYTICS = "true"

[[vm]]
  size = "shared-cpu-1x"
  memory = "2gb"

[processes]
  app = "/app/bin/run.sh syncer-postgres"
EOF

# Replace YOUR-USERNAME with actual username
sed -i 's/YOUR-USERNAME/your-actual-github-username/g' config/fly.toml config/fly.syncer.toml

# Commit configurations
git add config/
git commit -m "Add Fly.io configuration with custom image"
git push origin main
```

---

### Step 4: Create Deployment Scripts

**Duration:** 5 minutes

#### 4.1 Server Deployment Script

```bash
cat > scripts/deploy-server.sh << 'EOF'
#!/bin/bash
set -euo pipefail

echo "🚀 Deploying BemiDB Server to Fly.io..."

# Load environment variables
if [ -f .env ]; then
  export $(cat .env | grep -v '^#' | xargs)
fi

# Deploy server
cd config
flyctl deploy -c fly.toml -a bemidb-server

echo "✅ Server deployed successfully!"
echo "📡 Connection: postgresql://bemidb-server.fly.dev:5432/bemidb"
EOF

chmod +x scripts/deploy-server.sh
```

#### 4.2 Syncer Deployment Script

```bash
cat > scripts/deploy-syncer.sh << 'EOF'
#!/bin/bash
set -euo pipefail

echo "🚀 Deploying BemiDB Syncer to Fly.io..."

# Load environment variables
if [ -f .env ]; then
  export $(cat .env | grep -v '^#' | xargs)
fi

# Deploy syncer
cd config
flyctl deploy -c fly.syncer.toml -a bemidb-syncer

# Get machine ID
MACHINE_ID=$(flyctl machines list -a bemidb-syncer --json | jq -r '.[0].id')
echo "📝 Machine ID: $MACHINE_ID"
echo "💡 Save this for Cloud Scheduler setup"

echo "✅ Syncer deployed successfully!"
EOF

chmod +x scripts/deploy-syncer.sh
```

#### 4.3 Cloud Scheduler Setup Script

```bash
cat > scripts/setup-cloud-scheduler.sh << 'EOF'
#!/bin/bash
set -euo pipefail

# Load environment variables
if [ -f .env ]; then
  export $(cat .env | grep -v '^#' | xargs)
fi

# Validate required variables
: ${FLY_API_TOKEN:?FLY_API_TOKEN must be set}
: ${FLY_MACHINE_ID:?FLY_MACHINE_ID must be set}
: ${GCP_PROJECT_ID:?GCP_PROJECT_ID must be set}
: ${GCP_REGION:?GCP_REGION must be set}

echo "🔧 Setting up Google Cloud Scheduler jobs..."

# Set GCP project
gcloud config set project $GCP_PROJECT_ID

# Create hourly sync job
echo "Creating hourly sync job..."
gcloud scheduler jobs create http bemidb-hourly-sync \
  --schedule="0 * * * *" \
  --uri="https://api.machines.dev/v1/apps/bemidb-syncer/machines/${FLY_MACHINE_ID}/start" \
  --http-method=POST \
  --headers="Authorization=Bearer ${FLY_API_TOKEN},Content-Type=application/json" \
  --message-body='{
    "config": {
      "env": {
        "SOURCE_POSTGRES_INCLUDE_TABLES": "public.users,public.sessions,public.orders"
      }
    }
  }' \
  --location=$GCP_REGION \
  --time-zone="America/New_York" \
  --description="BemiDB hourly sync for fast-changing tables"

# Create daily sync job
echo "Creating daily sync job..."
gcloud scheduler jobs create http bemidb-daily-sync \
  --schedule="0 2 * * *" \
  --uri="https://api.machines.dev/v1/apps/bemidb-syncer/machines/${FLY_MACHINE_ID}/start" \
  --http-method=POST \
  --headers="Authorization=Bearer ${FLY_API_TOKEN},Content-Type=application/json" \
  --message-body='{
    "config": {
      "env": {
        "SOURCE_POSTGRES_INCLUDE_TABLES": "public.analytics,public.reports"
      }
    }
  }' \
  --location=$GCP_REGION \
  --time-zone="America/New_York" \
  --description="BemiDB daily sync for slow-changing tables"

echo "✅ Cloud Scheduler jobs created successfully!"
echo "📋 List jobs: gcloud scheduler jobs list --location=$GCP_REGION"
EOF

chmod +x scripts/setup-cloud-scheduler.sh
```

#### 4.4 Commit Scripts

```bash
git add scripts/
git commit -m "Add deployment automation scripts"
git push origin main
```

---

### Summary: Repository Structure

After completing these steps, you'll have:

**Repository 1: BemiDB Fork** (`YOUR-USERNAME/BemiDB`)
```
BemiDB/
├── .github/workflows/
│   └── docker-build.yml         # Auto-builds on push
├── src/
│   └── syncer-postgres/lib/
│       └── syncer_full_refresh.go  # DeleteOldTables commented out
├── Dockerfile
└── ...

Publishes to: ghcr.io/YOUR-USERNAME/bemidb:latest
```

**Repository 2: Deployment** (`YOUR-USERNAME/bemidb-deployment`)
```
bemidb-deployment/
├── config/
│   ├── fly.toml                 # Server config (uses your image)
│   ├── fly.syncer.toml          # Syncer config (uses your image)
│   └── .env.sample              # Template for secrets
├── scripts/
│   ├── deploy-server.sh         # Deploy server to Fly.io
│   ├── deploy-syncer.sh         # Deploy syncer to Fly.io
│   └── setup-cloud-scheduler.sh # Create GCP cron jobs
├── docs/
│   └── DEPLOYMENT.md            # Full deployment guide
└── README.md
```

---

## Infrastructure Setup

### Step 1: Set Up Neon PostgreSQL (Catalog Database)

**Duration:** 5 minutes

#### 1.1 Create Neon Project

```bash
# Option A: Via Neon Console (recommended for first-time users)
# 1. Go to https://console.neon.tech
# 2. Click "Create Project"
# 3. Project name: "bemidb-catalog"
# 4. Region: Choose closest to your Fly.io region (e.g., us-east-1)
# 5. Postgres version: 16
# 6. Click "Create Project"

# Option B: Via Neon CLI
npm install -g neonctl
neonctl auth
neonctl projects create --name bemidb-catalog --region aws-us-east-1
neonctl databases create catalog
```

#### 1.2 Get Connection String

```bash
# From Neon Console:
# 1. Navigate to your project
# 2. Click "Connection Details"
# 3. Copy the connection string

# Example format:
CATALOG_DATABASE_URL="postgresql://user:password@ep-cool-darkness-123456.us-east-1.aws.neon.tech/catalog?sslmode=require"

# Save this - you'll need it later
echo "CATALOG_DATABASE_URL=\"postgresql://user:password@ep-xxx.us-east-1.aws.neon.tech/catalog?sslmode=require\"" > .env.catalog
```

#### 1.3 Configure Neon Settings

```
In Neon Console → Settings:

Compute settings:
  - Autoscaling: Enable
  - Min compute: 0.25 vCPU
  - Max compute: 0.5 vCPU
  - Autosuspend delay: 5 minutes

Connection pooling:
  - Enable connection pooling: Yes
  - Mode: Transaction (recommended for BemiDB)

Storage:
  - No configuration needed (auto-scales)
```

#### 1.4 Verify Catalog Connection

```bash
# Source environment variables
source .env.catalog

# Test connection
psql "$CATALOG_DATABASE_URL" -c "SELECT version()"

# Expected output:
# PostgreSQL 16.x on x86_64-pc-linux-gnu, compiled by gcc...

# The catalog schema will be auto-created by BemiDB on first sync
```

---

### Step 2: Set Up Cloudflare R2 (Storage)

**Duration:** 5 minutes

#### 2.1 Create R2 Bucket

```bash
# Via Cloudflare Dashboard:
# 1. Go to https://dash.cloudflare.com
# 2. Navigate to R2 Object Storage
# 3. Click "Create bucket"
# 4. Bucket name: "bemidb-data"
# 5. Location: Automatic (R2 is globally distributed)
# 6. Click "Create bucket"
```

#### 2.2 Create R2 API Token

```bash
# Via Cloudflare Dashboard:
# 1. R2 → Manage R2 API Tokens
# 2. Click "Create API token"
# 3. Token name: "bemidb-access"
# 4. Permissions: Object Read & Write
# 5. Specify bucket: bemidb-data (recommended)
# 6. TTL: Never expire (or set expiration and create calendar reminder)
# 7. Click "Create API Token"

# Save the credentials (shown only once):
Access Key ID: abcdef1234567890abcdef1234567890
Secret Access Key: 1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcd
```

#### 2.3 Get R2 Configuration

```bash
# From bucket settings, note:
# 1. Bucket name: bemidb-data
# 2. Account ID: visible in URL or dashboard

# R2 endpoint format:
# https://<ACCOUNT_ID>.r2.cloudflarestorage.com

# Example:
AWS_S3_BUCKET="bemidb-data"
AWS_ACCESS_KEY_ID="abcdef1234567890abcdef1234567890"
AWS_SECRET_ACCESS_KEY="1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcd"
AWS_S3_ENDPOINT="https://a1b2c3d4e5f6.r2.cloudflarestorage.com"
AWS_REGION="auto"  # R2 uses "auto" for global distribution

# Save to file
cat >> .env.catalog << EOF
AWS_S3_BUCKET="bemidb-data"
AWS_ACCESS_KEY_ID="abcdef1234567890abcdef1234567890"
AWS_SECRET_ACCESS_KEY="1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcd"
AWS_S3_ENDPOINT="https://a1b2c3d4e5f6.r2.cloudflarestorage.com"
AWS_REGION="auto"
EOF
```

#### 2.4 Verify R2 Access

```bash
# Install AWS CLI (if not already installed)
pip install awscli

# Test R2 access
source .env.catalog

aws s3 ls s3://$AWS_S3_BUCKET \
  --endpoint-url $AWS_S3_ENDPOINT \
  --region auto

# Expected output (empty bucket):
# (no output = success, bucket is empty)

# Create test file
echo "test" > test.txt
aws s3 cp test.txt s3://$AWS_S3_BUCKET/test.txt \
  --endpoint-url $AWS_S3_ENDPOINT \
  --region auto

# List bucket
aws s3 ls s3://$AWS_S3_BUCKET \
  --endpoint-url $AWS_S3_ENDPOINT \
  --region auto

# Expected output:
# 2025-01-15 10:30:00          5 test.txt

# Clean up test file
aws s3 rm s3://$AWS_S3_BUCKET/test.txt \
  --endpoint-url $AWS_S3_ENDPOINT \
  --region auto
rm test.txt
```

---

### Step 3: Deploy BemiDB Server (Fly.io)

**Duration:** 10 minutes

#### 3.1 Create Fly.io Application for Server

```bash
# Navigate to BemiDB repository
cd /home/jc/git/BemiDB

# Create fly.toml for server
cat > fly.toml << 'EOF'
app = "bemidb-server"
primary_region = "iad"  # us-east-1, close to Neon

[build]
  image = "ghcr.io/bemihq/bemidb:latest"

[env]
  BEMIDB_PORT = "54321"
  BEMIDB_HOST = "0.0.0.0"
  BEMIDB_DATABASE = "bemidb"
  BEMIDB_LOG_LEVEL = "INFO"
  BEMIDB_DISABLE_ANONYMOUS_ANALYTICS = "true"
  AWS_REGION = "auto"

[[services]]
  internal_port = 54321
  protocol = "tcp"
  auto_stop_machines = false
  auto_start_machines = true
  min_machines_running = 1

  [[services.ports]]
    port = 5432
    handlers = ["tls"]

  [[services.ports]]
    port = 54321
    handlers = []

[[vm]]
  size = "shared-cpu-2x"
  memory = "4gb"

[http_service]
  internal_port = 54321
  force_https = false
  auto_stop_machines = false
  auto_start_machines = true
  min_machines_running = 1
EOF

# Initialize Fly.io app (don't deploy yet)
flyctl launch --no-deploy --name bemidb-server
```

#### 3.2 Set Server Secrets

```bash
# Source environment variables
source .env.catalog

# Add source database URL to env file
cat >> .env.catalog << EOF
SOURCE_POSTGRES_DATABASE_URL="postgresql://bemidb_syncer:password@your-db.example.com:5432/production"
EOF

source .env.catalog

# Set Fly.io secrets for server
flyctl secrets set \
  CATALOG_DATABASE_URL="$CATALOG_DATABASE_URL" \
  AWS_S3_BUCKET="$AWS_S3_BUCKET" \
  AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
  AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
  AWS_S3_ENDPOINT="$AWS_S3_ENDPOINT" \
  -a bemidb-server

# Verify secrets are set
flyctl secrets list -a bemidb-server
```

#### 3.3 Deploy BemiDB Server

```bash
# Deploy server
flyctl deploy -a bemidb-server

# Wait for deployment to complete (1-2 minutes)
# Watch logs in real-time
flyctl logs -a bemidb-server

# Expected log output:
# [INFO] BemiDB: Listening on 0.0.0.0:54321
# [INFO] DuckDB: Connected
```

#### 3.4 Verify Server Deployment

```bash
# Get server info
flyctl info -a bemidb-server

# Example output:
# Name     = bemidb-server
# Owner    = your-org
# Hostname = bemidb-server.fly.dev
# Image    = ghcr.io/bemihq/bemidb:latest
# Platform = machines
# ...

# Test connection (should work even with no data yet)
psql postgresql://bemidb-server.fly.dev:5432/bemidb -c "SELECT 1"

# Expected output:
#  ?column?
# ----------
#         1
# (1 row)

# List tables (should be empty initially)
psql postgresql://bemidb-server.fly.dev:5432/bemidb -c \
  "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog', 'information_schema')"

# Expected output:
#  table_schema | table_name
# --------------+------------
# (0 rows)
```

---

### Step 4: Deploy BemiDB Syncer Machine (Fly.io)

**Duration:** 10 minutes

#### 4.1 Create Fly.io Application for Syncer

```bash
# Create fly.syncer.toml
cat > fly.syncer.toml << 'EOF'
app = "bemidb-syncer"
primary_region = "iad"  # Same region as server

[build]
  image = "ghcr.io/bemihq/bemidb:latest"

[env]
  DESTINATION_SCHEMA_NAME = "postgres"
  AWS_REGION = "auto"
  BEMIDB_LOG_LEVEL = "INFO"
  BEMIDB_DISABLE_ANONYMOUS_ANALYTICS = "true"

[[vm]]
  size = "shared-cpu-1x"
  memory = "2gb"

[processes]
  app = "/app/bin/run.sh syncer-postgres"
EOF

# Initialize syncer app (don't deploy yet)
flyctl launch --no-deploy --name bemidb-syncer -c fly.syncer.toml
```

#### 4.2 Set Syncer Secrets

```bash
# Syncer needs same secrets as server, plus source database URL
source .env.catalog

flyctl secrets set \
  SOURCE_POSTGRES_DATABASE_URL="$SOURCE_POSTGRES_DATABASE_URL" \
  CATALOG_DATABASE_URL="$CATALOG_DATABASE_URL" \
  AWS_S3_BUCKET="$AWS_S3_BUCKET" \
  AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
  AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
  AWS_S3_ENDPOINT="$AWS_S3_ENDPOINT" \
  -a bemidb-syncer

# Verify secrets
flyctl secrets list -a bemidb-syncer
```

#### 4.3 Deploy Syncer Machine

```bash
# Deploy syncer
flyctl deploy -a bemidb-syncer -c fly.syncer.toml

# Wait for deployment
flyctl status -a bemidb-syncer

# Machine will start and likely run a sync immediately (if SOURCE_POSTGRES_INCLUDE_TABLES is not set)
# This is OK - we'll configure it properly in the next steps
```

#### 4.4 Get Machine ID

```bash
# List machines for syncer app
flyctl machines list -a bemidb-syncer

# Example output:
# ID              NAME            STATE   REGION  IMAGE                               ...
# 148ed123abc456  bemidb-syncer   stopped iad     ghcr.io/bemihq/bemidb:latest       ...
#         ^^^^^^^^^^^^^^
#         Save this Machine ID - you'll need it for Cloud Scheduler

# Save to environment
export FLY_MACHINE_ID="148ed123abc456"
echo "FLY_MACHINE_ID=\"148ed123abc456\"" >> .env.catalog
```

#### 4.5 Configure Machine to Stop After Execution

```bash
# Stop the machine (if running)
flyctl machine stop $FLY_MACHINE_ID -a bemidb-syncer

# Verify machine is stopped
flyctl machines list -a bemidb-syncer

# Expected output:
# ID              STATE   ...
# 148ed123abc456  stopped ...
```

---

### Step 5: Create Fly.io API Token

**Duration:** 2 minutes

#### 5.1 Generate Deploy Token

```bash
# Create a deploy token (scoped to deployments)
flyctl tokens create deploy

# Example output:
# FlyV1 fm1_abc123def456ghi789jkl012mno345pqr678stu901vwx234yz

# Save this token securely
export FLY_API_TOKEN="FlyV1 fm1_abc123def456ghi789jkl012mno345pqr678stu901vwx234yz"
echo "FLY_API_TOKEN=\"FlyV1 fm1_abc123def456ghi789jkl012mno345pqr678stu901vwx234yz\"" >> .env.catalog

# IMPORTANT: Keep this token secure!
# - It allows starting/stopping your Fly machines
# - Don't commit to Git
# - Store in password manager
```

#### 5.2 Test API Token

```bash
# Test token by calling Fly Machines API
source .env.catalog

curl -X GET \
  -H "Authorization: Bearer $FLY_API_TOKEN" \
  -H "Content-Type: application/json" \
  "https://api.machines.dev/v1/apps/bemidb-syncer/machines"

# Expected output (JSON array of machines):
# [
#   {
#     "id": "148ed123abc456",
#     "name": "bemidb-syncer",
#     "state": "stopped",
#     "region": "iad",
#     ...
#   }
# ]
```

---

## Google Cloud Scheduler Setup

### Step 1: Set Up Google Cloud Project

**Duration:** 5 minutes

#### 1.1 Create or Select Project

```bash
# Authenticate with Google Cloud
gcloud auth login

# List existing projects
gcloud projects list

# Option A: Create new project
gcloud projects create bemidb-scheduler --name="BemiDB Scheduler"
gcloud config set project bemidb-scheduler

# Option B: Use existing project
gcloud config set project YOUR-EXISTING-PROJECT-ID

# Enable required APIs
gcloud services enable cloudscheduler.googleapis.com
gcloud services enable logging.googleapis.com

# Set default region (close to your Fly.io region)
gcloud config set scheduler/location us-east1
```

#### 1.2 Verify Cloud Scheduler Setup

```bash
# List Cloud Scheduler jobs (should be empty initially)
gcloud scheduler jobs list --location=us-east1

# Expected output:
# Listed 0 items.
```

---

### Step 2: Create Cloud Scheduler Jobs

**Duration:** 10 minutes

#### 2.1 Source Environment Variables

```bash
# Make sure all variables are loaded
source .env.catalog

# Verify critical variables
echo "Machine ID: $FLY_MACHINE_ID"
echo "API Token: ${FLY_API_TOKEN:0:20}..." # Show first 20 chars only

# If any are empty, set them now:
# export FLY_MACHINE_ID="148ed123abc456"
# export FLY_API_TOKEN="FlyV1 fm1_..."
```

#### 2.2 Create Hourly Sync Job (Fast-Changing Tables)

```bash
# Create Cloud Scheduler job for hourly sync
gcloud scheduler jobs create http bemidb-hourly-sync \
  --schedule="0 * * * *" \
  --uri="https://api.machines.dev/v1/apps/bemidb-syncer/machines/${FLY_MACHINE_ID}/start" \
  --http-method=POST \
  --headers="Authorization=Bearer ${FLY_API_TOKEN},Content-Type=application/json" \
  --message-body='{
    "config": {
      "env": {
        "SOURCE_POSTGRES_INCLUDE_TABLES": "public.users,public.sessions,public.orders"
      }
    }
  }' \
  --location=us-east1 \
  --time-zone="America/New_York" \
  --description="BemiDB hourly sync for fast-changing tables"

# Verify job created
gcloud scheduler jobs describe bemidb-hourly-sync --location=us-east1
```

**Explanation:**
- `--schedule="0 * * * *"` - Every hour at minute 0 (1:00, 2:00, 3:00...)
- `--uri` - Fly Machines API endpoint to start machine
- `--headers` - Authorization with Fly API token
- `--message-body` - JSON with environment variables to inject
- `--time-zone` - Adjust to your timezone (UTC, America/New_York, Europe/London, etc.)

#### 2.3 Create Daily Sync Job (Slow-Changing Tables)

```bash
# Create Cloud Scheduler job for daily sync
gcloud scheduler jobs create http bemidb-daily-sync \
  --schedule="0 2 * * *" \
  --uri="https://api.machines.dev/v1/apps/bemidb-syncer/machines/${FLY_MACHINE_ID}/start" \
  --http-method=POST \
  --headers="Authorization=Bearer ${FLY_API_TOKEN},Content-Type=application/json" \
  --message-body='{
    "config": {
      "env": {
        "SOURCE_POSTGRES_INCLUDE_TABLES": "public.analytics,public.reports,public.archives"
      }
    }
  }' \
  --location=us-east1 \
  --time-zone="America/New_York" \
  --description="BemiDB daily sync for slow-changing tables"

# Verify job created
gcloud scheduler jobs describe bemidb-daily-sync --location=us-east1
```

**Explanation:**
- `--schedule="0 2 * * *"` - Daily at 2:00 AM
- Different `SOURCE_POSTGRES_INCLUDE_TABLES` - Syncs different tables than hourly job

#### 2.4 Create Weekly Sync Job (Archives, Optional)

```bash
# Create Cloud Scheduler job for weekly sync
gcloud scheduler jobs create http bemidb-weekly-sync \
  --schedule="0 3 * * 0" \
  --uri="https://api.machines.dev/v1/apps/bemidb-syncer/machines/${FLY_MACHINE_ID}/start" \
  --http-method=POST \
  --headers="Authorization=Bearer ${FLY_API_TOKEN},Content-Type=application/json" \
  --message-body='{
    "config": {
      "env": {
        "SOURCE_POSTGRES_INCLUDE_TABLES": "public.audit_logs,public.old_data"
      }
    }
  }' \
  --location=us-east1 \
  --time-zone="America/New_York" \
  --description="BemiDB weekly sync for archive tables"

# Verify job created
gcloud scheduler jobs describe bemidb-weekly-sync --location=us-east1
```

**Explanation:**
- `--schedule="0 3 * * 0"` - Weekly on Sunday at 3:00 AM
- Syncs infrequently-changing tables

#### 2.5 List All Created Jobs

```bash
# List all scheduler jobs
gcloud scheduler jobs list --location=us-east1

# Expected output:
# ID                     LOCATION   SCHEDULE (TZ)              TARGET_TYPE  STATE
# bemidb-hourly-sync     us-east1   0 * * * * (America/New_York)  HTTP        ENABLED
# bemidb-daily-sync      us-east1   0 2 * * * (America/New_York)  HTTP        ENABLED
# bemidb-weekly-sync     us-east1   0 3 * * 0 (America/New_York)  HTTP        ENABLED
```

---

## Testing & Verification

### Step 1: Manual Test First Sync

**Duration:** 5-10 minutes

#### 1.1 Trigger Hourly Sync Manually

```bash
# Trigger the hourly sync job manually (don't wait for schedule)
gcloud scheduler jobs run bemidb-hourly-sync --location=us-east1

# Expected output:
# Triggering job [bemidb-hourly-sync]...done.

# The job will make an HTTP POST to Fly Machines API
# This will start the bemidb-syncer machine
```

#### 1.2 Watch Fly.io Syncer Logs

```bash
# Watch logs in real-time (in a separate terminal)
flyctl logs -a bemidb-syncer

# Expected output:
# 2025-01-15T10:30:00Z Starting Syncer for PostgreSQL...
# 2025-01-15T10:30:05Z [Syncer] INFO: Connecting to source database
# 2025-01-15T10:30:07Z [Syncer] INFO: Connected successfully
# 2025-01-15T10:30:10Z [Syncer] INFO: Found 3 tables to sync: users, sessions, orders
# 2025-01-15T10:30:12Z [Syncer] INFO: Syncing public.users
# 2025-01-15T10:30:15Z [Syncer] INFO: Extracted 50000 rows from users
# 2025-01-15T10:30:18Z [Syncer] INFO: Converted to Parquet: users/data/00001.parquet (2.3 MB, compressed)
# 2025-01-15T10:30:25Z [Syncer] INFO: Uploaded to s3://bemidb-data/iceberg/postgres/users/data/00001.parquet
# 2025-01-15T10:30:27Z [Syncer] INFO: Syncing public.sessions
# ... (continues for each table)
# 2025-01-15T10:31:45Z [Syncer] INFO: Catalog updated successfully
# 2025-01-15T10:31:46Z [Syncer] INFO: Sync completed. Total time: 106 seconds
# 2025-01-15T10:31:47Z Syncer for PostgreSQL finished.
```

#### 1.3 Check Machine Status

```bash
# Check if machine stopped after sync
flyctl machines list -a bemidb-syncer

# Expected output (after sync completes):
# ID              STATE   ...
# 148ed123abc456  stopped ...

# If state is "started", machine is still running
# If state is "stopped", sync completed and machine auto-stopped
```

#### 1.4 Verify Data in Catalog

```bash
# Query catalog database to see synced tables
source .env.catalog

psql "$CATALOG_DATABASE_URL" -c "SELECT table_namespace, table_name FROM iceberg_tables"

# Expected output:
#  table_namespace | table_name
# -----------------+------------
#  postgres        | users
#  postgres        | sessions
#  postgres        | orders
# (3 rows)
```

#### 1.5 Verify Data in R2

```bash
# List Parquet files in R2 bucket
aws s3 ls s3://$AWS_S3_BUCKET/iceberg/postgres/ \
  --recursive \
  --endpoint-url $AWS_S3_ENDPOINT \
  --region auto

# Expected output (example):
# 2025-01-15 10:30:25    2400000 iceberg/postgres/users/data/00001-abc123.parquet
# 2025-01-15 10:30:48    5100000 iceberg/postgres/sessions/data/00001-def456.parquet
# 2025-01-15 10:31:22    3800000 iceberg/postgres/orders/data/00001-ghi789.parquet
# 2025-01-15 10:31:45       1234 iceberg/postgres/users/metadata/v1.metadata.json
# ... (metadata files for each table)
```

#### 1.6 Query Data via BemiDB Server

```bash
# List tables visible to BemiDB server
psql postgresql://bemidb-server.fly.dev:5432/bemidb -c \
  "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema = 'postgres'"

# Expected output:
#  table_schema | table_name
# --------------+------------
#  postgres     | users
#  postgres     | sessions
#  postgres     | orders
# (3 rows)

# Query actual data
psql postgresql://bemidb-server.fly.dev:5432/bemidb -c \
  "SELECT COUNT(*) FROM postgres.users"

# Expected output:
#  count
# -------
#  50000
# (1 row)

# Test a real query
psql postgresql://bemidb-server.fly.dev:5432/bemidb -c \
  "SELECT id, email, created_at FROM postgres.users LIMIT 5"

# Expected output:
#  id  |       email        |      created_at
# -----+--------------------+---------------------
#    1 | user1@example.com  | 2024-01-15 08:30:00
#    2 | user2@example.com  | 2024-01-16 10:15:00
# ... (5 rows)
```

---

### Step 2: Verify Cloud Scheduler Execution

**Duration:** 2 minutes

#### 2.1 Check Job Execution History

```bash
# View execution history for hourly sync
gcloud scheduler jobs describe bemidb-hourly-sync --location=us-east1

# Look for fields:
# state: ENABLED
# lastAttemptTime: '2025-01-15T10:30:00Z'
# status:
#   code: 0  # 0 = success
#   message: Success
```

#### 2.2 View Cloud Scheduler Logs

```bash
# View logs for the job
gcloud logging read "resource.type=cloud_scheduler_job AND resource.labels.job_id=bemidb-hourly-sync" \
  --limit=10 \
  --format=json

# Expected log entry:
# {
#   "severity": "INFO",
#   "textPayload": "Execution succeeded",
#   "timestamp": "2025-01-15T10:30:00.123Z",
#   ...
# }
```

---

### Step 3: Test Different Sync Jobs

**Duration:** 5-10 minutes (optional)

#### 3.1 Trigger Daily Sync

```bash
# Manually trigger daily sync
gcloud scheduler jobs run bemidb-daily-sync --location=us-east1

# Watch logs
flyctl logs -a bemidb-syncer

# Should sync different tables: analytics, reports, archives
```

#### 3.2 Verify All Tables Coexist

```bash
# Query BemiDB to see all tables
psql postgresql://bemidb-server.fly.dev:5432/bemidb -c \
  "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema = 'postgres' ORDER BY table_name"

# Expected output (all tables from both syncs):
#  table_schema | table_name
# --------------+------------
#  postgres     | analytics
#  postgres     | archives
#  postgres     | orders
#  postgres     | reports
#  postgres     | sessions
#  postgres     | users
# (6 rows)

# This confirms DeleteOldTables() removal is working!
# Tables from hourly sync (users, sessions, orders) were NOT deleted by daily sync
```

---

## Multiple Sync Schedules

### Common Scheduling Patterns

#### Pattern 1: Frequency-Based (Recommended)

**Hourly** - Fast-changing, frequently queried tables:
```bash
# Every hour
--schedule="0 * * * *"
--message-body='{
  "config": {
    "env": {
      "SOURCE_POSTGRES_INCLUDE_TABLES": "public.users,public.sessions,public.orders,public.events"
    }
  }
}'
```

**Every 6 Hours** - Medium-changing tables:
```bash
# 0:00, 6:00, 12:00, 18:00
--schedule="0 */6 * * *"
--message-body='{
  "config": {
    "env": {
      "SOURCE_POSTGRES_INCLUDE_TABLES": "public.products,public.inventory,public.customers"
    }
  }
}'
```

**Daily** - Slow-changing, analytical tables:
```bash
# Daily at 2:00 AM
--schedule="0 2 * * *"
--message-body='{
  "config": {
    "env": {
      "SOURCE_POSTGRES_INCLUDE_TABLES": "public.analytics,public.reports,public.metrics"
    }
  }
}'
```

**Weekly** - Archives, rarely-changing tables:
```bash
# Sunday at 3:00 AM
--schedule="0 3 * * 0"
--message-body='{
  "config": {
    "env": {
      "SOURCE_POSTGRES_INCLUDE_TABLES": "public.audit_logs,public.archives,public.backups"
    }
  }
}'
```

---

#### Pattern 2: Business-Logic Based

**Business Hours** (High activity):
```bash
# Every 30 minutes during business hours (9 AM - 5 PM weekdays)
--schedule="*/30 9-17 * * 1-5"
```

**Off-Hours** (Low activity):
```bash
# Every 4 hours outside business hours
--schedule="0 */4 * * *"
```

**Weekend** (Special processing):
```bash
# Saturday at midnight for weekly aggregations
--schedule="0 0 * * 6"
```

---

#### Pattern 3: Data Size Based

**Small Tables** (< 100 MB):
```bash
# Sync frequently (every 30 minutes)
--schedule="*/30 * * * *"
--message-body='{
  "config": {
    "env": {
      "SOURCE_POSTGRES_INCLUDE_TABLES": "public.users,public.settings"
    }
  }
}'
```

**Large Tables** (> 1 GB):
```bash
# Sync less frequently (daily)
--schedule="0 2 * * *"
--message-body='{
  "config": {
    "env": {
      "SOURCE_POSTGRES_INCLUDE_TABLES": "public.events,public.logs"
    }
  }
}'
```

---

### Managing Multiple Schedules

#### Create Multiple Jobs for Same Tables (Different Times)

```bash
# Critical table synced every 15 minutes
gcloud scheduler jobs create http bemidb-users-15min \
  --schedule="*/15 * * * *" \
  --uri="https://api.machines.dev/v1/apps/bemidb-syncer/machines/${FLY_MACHINE_ID}/start" \
  --headers="Authorization=Bearer ${FLY_API_TOKEN},Content-Type=application/json" \
  --message-body='{"config":{"env":{"SOURCE_POSTGRES_INCLUDE_TABLES":"public.users"}}}' \
  --location=us-east1

# Same table, overnight full refresh
gcloud scheduler jobs create http bemidb-users-nightly \
  --schedule="0 1 * * *" \
  --uri="https://api.machines.dev/v1/apps/bemidb-syncer/machines/${FLY_MACHINE_ID}/start" \
  --headers="Authorization=Bearer ${FLY_API_TOKEN},Content-Type=application/json" \
  --message-body='{"config":{"env":{"SOURCE_POSTGRES_INCLUDE_TABLES":"public.users"}}}' \
  --location=us-east1
```

**Note:** Multiple syncs of the same table are safe. Each sync:
1. Creates temporary table (e.g., `users-syncing`)
2. Writes new data
3. Atomically swaps tables
4. Deletes old version

Latest sync always wins, no conflicts.

---

### Cron Expression Reference

```
┌───────────── minute (0 - 59)
│ ┌───────────── hour (0 - 23)
│ │ ┌───────────── day of month (1 - 31)
│ │ │ ┌───────────── month (1 - 12)
│ │ │ │ ┌───────────── day of week (0 - 6) (Sunday=0)
│ │ │ │ │
* * * * *
```

**Common Expressions:**

| Schedule | Cron Expression | Description |
|----------|----------------|-------------|
| Every 15 minutes | `*/15 * * * *` | Runs at :00, :15, :30, :45 |
| Every hour | `0 * * * *` | Runs at :00 |
| Every 6 hours | `0 */6 * * *` | Runs at 0:00, 6:00, 12:00, 18:00 |
| Daily at 2 AM | `0 2 * * *` | Runs once per day |
| Weekdays at 9 AM | `0 9 * * 1-5` | Monday-Friday only |
| First day of month | `0 0 1 * *` | Monthly at midnight |
| Sunday at 3 AM | `0 3 * * 0` | Weekly |

---

## Monitoring & Logging

### Cloud Scheduler Monitoring

#### View Job Status Dashboard

```bash
# Via Google Cloud Console:
# 1. Go to https://console.cloud.google.com
# 2. Navigate to Cloud Scheduler
# 3. View all jobs with status

# Via CLI:
gcloud scheduler jobs list --location=us-east1

# Example output:
# ID                     LOCATION   SCHEDULE              STATE    LAST_RUN
# bemidb-hourly-sync     us-east1   0 * * * *            ENABLED  2025-01-15T14:00:00Z (Success)
# bemidb-daily-sync      us-east1   0 2 * * *            ENABLED  2025-01-15T02:00:00Z (Success)
```

#### View Recent Executions

```bash
# View last 10 executions of hourly sync
gcloud logging read "resource.type=cloud_scheduler_job AND resource.labels.job_id=bemidb-hourly-sync" \
  --limit=10 \
  --format="table(timestamp,severity,jsonPayload.message)"

# Example output:
# TIMESTAMP                SEVERITY  MESSAGE
# 2025-01-15T14:00:00.123Z INFO      Execution succeeded
# 2025-01-15T13:00:00.456Z INFO      Execution succeeded
# 2025-01-15T12:00:00.789Z INFO      Execution succeeded
```

---

### Fly.io Syncer Monitoring

#### Real-Time Logs

```bash
# Watch logs live
flyctl logs -a bemidb-syncer

# Filter for errors only
flyctl logs -a bemidb-syncer | grep ERROR

# Search historical logs
flyctl logs -a bemidb-syncer --search "Sync completed"
```

#### Machine Metrics

```bash
# View machine status
flyctl machines list -a bemidb-syncer

# View resource usage (while running)
flyctl status -a bemidb-syncer

# Example output:
# Machines
# ID              STATE   REGION  HEALTH  CHECKS  IMAGE                               CREATED
# 148ed123abc456  stopped iad     -       -       ghcr.io/bemihq/bemidb:latest       2h ago
```

---

### BemiDB Server Monitoring

#### Connection Monitoring

```bash
# Check active connections
psql postgresql://bemidb-server.fly.dev:5432/bemidb -c \
  "SELECT count(*) FROM pg_stat_activity WHERE datname = 'bemidb'"

# Example output:
#  count
# -------
#      3
# (1 row)
```

#### Query Performance

```bash
# View slow queries (if any)
psql postgresql://bemidb-server.fly.dev:5432/bemidb -c \
  "SELECT query, query_start, state FROM pg_stat_activity WHERE state = 'active' ORDER BY query_start"
```

---

### Set Up Alerting

#### Cloud Scheduler Failure Alerts

```bash
# Create alert policy for failed jobs (via Cloud Console)
# 1. Go to Monitoring → Alerting
# 2. Create Policy
# 3. Condition: Cloud Scheduler Job > Execution Failed
# 4. Notification: Email, Slack, PagerDuty, etc.

# Or via gcloud:
gcloud alpha monitoring policies create \
  --notification-channels=CHANNEL_ID \
  --display-name="BemiDB Sync Failures" \
  --condition-display-name="Scheduler Job Failed" \
  --condition-threshold-value=1 \
  --condition-threshold-duration=0s \
  --condition-filter='resource.type="cloud_scheduler_job" AND metric.type="logging.googleapis.com/user/scheduler/job/execution_failed"'
```

#### Slack Notifications (Custom)

Create a Cloud Function to send Slack notifications:

```javascript
// index.js
const axios = require('axios');

exports.notifySlack = async (req, res) => {
  const { job_id, status, timestamp } = req.body;

  if (status !== 'success') {
    await axios.post(process.env.SLACK_WEBHOOK_URL, {
      text: `⚠️ BemiDB sync failed!`,
      attachments: [{
        color: 'danger',
        fields: [
          { title: 'Job', value: job_id, short: true },
          { title: 'Status', value: status, short: true },
          { title: 'Time', value: timestamp, short: false }
        ]
      }]
    });
  }

  res.status(200).send('OK');
};
```

Deploy:
```bash
gcloud functions deploy notify-slack \
  --runtime nodejs20 \
  --trigger-http \
  --set-secrets SLACK_WEBHOOK_URL=slack-webhook:latest
```

Update scheduler to call Cloud Function on failure (requires additional setup).

---

## Cost Breakdown

### Monthly Infrastructure Costs

| Service | Usage | Cost |
|---------|-------|------|
| **Fly.io Server** | 1x shared-cpu-2x (4GB) 24/7 | ~$30/month |
| **Fly.io Syncer** | Pay-per-second execution | See breakdown below |
| **Neon Catalog** | 0.25 vCPU, 1GB storage, autosuspend | ~$5/month (or free tier) |
| **Cloudflare R2** | 100GB storage, unlimited egress | ~$1.50/month |
| **Google Cloud Scheduler** | 3 jobs | $0.30/month |
| **TOTAL** | | **~$37-62/month** |

---

### Fly.io Syncer Cost Calculation

**Pricing:**
- Shared CPU 1x: $0.0000008/second (~$2.08/month if always on)
- Memory (2GB): $0.0000002/GB/second (~$1.04/GB/month if always on)
- **Total per second**: ~$0.0003/second

**Cost Examples:**

#### Scenario 1: Hourly Sync (Small Database)
```
Sync duration: 2 minutes average
Runs per day: 24
Monthly runs: 720

Runtime per month: 720 × 2 min × 60 sec = 86,400 seconds = 24 hours
Cost: 24 hours × 3600 sec × $0.0003 = $25.92/month
```

#### Scenario 2: Mixed Schedule (Optimized)
```
Hourly sync (fast tables): 1 min × 24 × 30 = 43,200 sec
Daily sync (slow tables): 5 min × 1 × 30 = 9,000 sec
Weekly sync (archives): 10 min × 4 = 2,400 sec

Total runtime: 54,600 seconds = 15.17 hours
Cost: 54,600 × $0.0003 = $16.38/month
```

#### Scenario 3: Every 6 Hours (Medium Frequency)
```
Sync duration: 5 minutes average
Runs per day: 4
Monthly runs: 120

Runtime per month: 120 × 5 min × 60 sec = 36,000 seconds = 10 hours
Cost: 36,000 × $0.0003 = $10.80/month
```

#### Scenario 4: Daily Only
```
Sync duration: 10 minutes average
Runs per day: 1
Monthly runs: 30

Runtime per month: 30 × 10 min × 60 sec = 18,000 seconds = 5 hours
Cost: 18,000 × $0.0003 = $5.40/month
```

---

### Cost Comparison with Alternatives

| Solution | Monthly Cost | Notes |
|----------|-------------|-------|
| **BemiDB (this setup)** | $37-62 | Server + Syncer + Storage + Scheduler |
| **GitHub Actions** | $36-40 | Cheaper by $1-22, but less reliable timing |
| **Render Cron** | $44-67 | Always-on containers, simpler setup |
| **Snowflake + Fivetran** | $300-1,000 | Enterprise features, much higher cost |
| **BigQuery + Data Transfer** | $100-500 | Google ecosystem, higher cost |

**BemiDB Advantage:** 10-25x cheaper than enterprise alternatives

---

### Cost Optimization Tips

1. **Reduce sync frequency** for less critical tables
   - Hourly → Every 6 hours: Save ~75% on syncer costs
   - Daily → Weekly: Save ~85% on syncer costs

2. **Optimize sync duration**
   - Use `SOURCE_POSTGRES_EXCLUDE_TABLES` for large audit logs
   - Sync only changed data (requires incremental mode, not yet implemented)

3. **Use Neon free tier**
   - Free: 0.5GB storage, autosuspend enabled
   - Catalog database typically < 100MB

4. **Batch small tables together**
   - Sync multiple small tables in one job (reduces overhead)

5. **Right-size Fly.io machines**
   - Server: shared-cpu-2x (4GB) is optimal for DuckDB
   - Syncer: shared-cpu-1x (2GB) sufficient for most workloads

---

## Troubleshooting

### Common Issues

#### Issue 1: Cloud Scheduler Job Fails with "Unauthorized"

**Symptoms:**
```
Execution failed: Unauthorized
```

**Diagnosis:**
```bash
# Check Fly API token
curl -X GET \
  -H "Authorization: Bearer $FLY_API_TOKEN" \
  "https://api.machines.dev/v1/apps/bemidb-syncer/machines"

# If error: "Unauthorized"
```

**Solutions:**
- Regenerate Fly API token: `flyctl tokens create deploy`
- Update Cloud Scheduler job with new token:
  ```bash
  gcloud scheduler jobs update http bemidb-hourly-sync \
    --headers="Authorization=Bearer NEW_TOKEN,Content-Type=application/json" \
    --location=us-east1
  ```

---

#### Issue 2: Fly Machine Doesn't Start

**Symptoms:**
```
Cloud Scheduler shows success, but Fly machine stays stopped
```

**Diagnosis:**
```bash
# Check machine ID is correct
flyctl machines list -a bemidb-syncer

# Check Fly logs
flyctl logs -a bemidb-syncer
```

**Solutions:**
- Verify `FLY_MACHINE_ID` in Cloud Scheduler URL matches actual machine
- Check machine isn't in error state: `flyctl machines restart $MACHINE_ID -a bemidb-syncer`
- Manually test Fly API:
  ```bash
  curl -X POST \
    -H "Authorization: Bearer $FLY_API_TOKEN" \
    -H "Content-Type: application/json" \
    "https://api.machines.dev/v1/apps/bemidb-syncer/machines/$MACHINE_ID/start"
  ```

---

#### Issue 3: Sync Fails with "Connection Refused" to Source Database

**Symptoms:**
```
[Syncer] ERROR: connection refused at source-db.example.com:5432
```

**Diagnosis:**
```bash
# Test source connection from Fly region
flyctl ssh console -a bemidb-syncer
# Inside container:
psql $SOURCE_POSTGRES_DATABASE_URL -c "SELECT 1"
```

**Solutions:**
- **Firewall**: Allow connections from Fly.io IP ranges
  - Get Fly IPs: https://fly.io/docs/reference/regions/#ipv4-addresses
  - Add to database firewall allowlist

- **SSL Mode**: Try different SSL modes
  ```bash
  # In Cloud Scheduler message body:
  "SOURCE_POSTGRES_DATABASE_URL": "postgresql://user:pass@host:5432/db?sslmode=require"
  # or
  "SOURCE_POSTGRES_DATABASE_URL": "postgresql://user:pass@host:5432/db?sslmode=disable"
  ```

- **VPN/Tailscale**: For private databases, set up Tailscale on Fly machine

---

#### Issue 4: Sync Takes Too Long, Times Out

**Symptoms:**
```
Machine runs for > 30 minutes
Large tables timeout
```

**Diagnosis:**
```bash
# Check which table is slow
flyctl logs -a bemidb-syncer | grep "Syncing"

# Example output:
# [Syncer] INFO: Syncing public.users (takes 2 min)
# [Syncer] INFO: Syncing public.events (takes 45 min) ← Problem table
```

**Solutions:**
- **Exclude large tables** from frequent syncs:
  ```bash
  # Hourly sync: exclude events
  "SOURCE_POSTGRES_INCLUDE_TABLES": "public.users,public.orders"

  # Daily sync: include events
  "SOURCE_POSTGRES_INCLUDE_TABLES": "public.events"
  ```

- **Increase machine size** temporarily:
  ```bash
  # Update fly.syncer.toml
  [[vm]]
    size = "shared-cpu-2x"  # Was: shared-cpu-1x
    memory = "4gb"          # Was: 2gb

  # Redeploy
  flyctl deploy -a bemidb-syncer -c fly.syncer.toml
  ```

- **Batch large tables** separately (sync one at a time)

---

#### Issue 5: Catalog Database Out of Connections

**Symptoms:**
```
ERROR: remaining connection slots are reserved
```

**Diagnosis:**
```bash
# Check active connections
psql "$CATALOG_DATABASE_URL" -c \
  "SELECT count(*) FROM pg_stat_activity WHERE datname = 'catalog'"
```

**Solutions:**
- **Enable Neon connection pooling**:
  - Neon Console → Project → Connection pooling → Enable
  - Use pooled connection string

- **Close stale connections**:
  ```sql
  SELECT pg_terminate_backend(pid) FROM pg_stat_activity
  WHERE datname = 'catalog' AND state = 'idle' AND state_change < now() - interval '10 minutes';
  ```

- **Increase Neon compute**: Scale up to 0.5 vCPU (allows more connections)

---

#### Issue 6: R2 Upload Fails with "Access Denied"

**Symptoms:**
```
ERROR: S3 operation failed: Access Denied
```

**Diagnosis:**
```bash
# Test R2 credentials
aws s3 ls s3://$AWS_S3_BUCKET \
  --endpoint-url $AWS_S3_ENDPOINT \
  --region auto
```

**Solutions:**
- **Check R2 token permissions**: Must have "Object Read & Write"
- **Verify bucket name**: `AWS_S3_BUCKET` matches actual bucket
- **Check token expiration**: Regenerate if expired
- **Update Fly secrets**:
  ```bash
  flyctl secrets set \
    AWS_ACCESS_KEY_ID="new-key" \
    AWS_SECRET_ACCESS_KEY="new-secret" \
    -a bemidb-syncer
  ```

---

#### Issue 7: Tables Missing from BemiDB Server

**Symptoms:**
```sql
SELECT * FROM postgres.users;
-- ERROR: relation "postgres.users" does not exist
```

**Diagnosis:**
```bash
# Check catalog has metadata
psql "$CATALOG_DATABASE_URL" -c \
  "SELECT table_namespace, table_name FROM iceberg_tables WHERE table_name = 'users'"

# Check R2 has Parquet files
aws s3 ls s3://$AWS_S3_BUCKET/iceberg/postgres/users/ \
  --recursive \
  --endpoint-url $AWS_S3_ENDPOINT
```

**Solutions:**
- **Catalog missing metadata**: Re-run sync
- **R2 missing files**: Check syncer logs for upload errors
- **Server can't read R2**: Verify server has correct `AWS_S3_ENDPOINT` secret
- **Restart BemiDB server**: Sometimes needed to reload catalog
  ```bash
  flyctl apps restart bemidb-server
  ```

---

### Debug Mode

**Enable TRACE logging:**

```bash
# Update Cloud Scheduler job to include TRACE logging
gcloud scheduler jobs update http bemidb-hourly-sync \
  --message-body='{
    "config": {
      "env": {
        "SOURCE_POSTGRES_INCLUDE_TABLES": "public.users",
        "BEMIDB_LOG_LEVEL": "TRACE"
      }
    }
  }' \
  --location=us-east1

# Trigger job
gcloud scheduler jobs run bemidb-hourly-sync --location=us-east1

# View detailed logs
flyctl logs -a bemidb-syncer
```

**TRACE logs include:**
- SQL queries sent to source database
- Parquet file creation details
- S3 upload progress
- Catalog update statements

---

## Maintenance

### Regular Tasks

#### Weekly

- [ ] **Check sync success rate**
  ```bash
  gcloud scheduler jobs list --location=us-east1
  # Verify all jobs show "Success" in LAST_RUN
  ```

- [ ] **Review Fly.io costs**
  ```bash
  # Check Fly.io billing dashboard
  # https://fly.io/dashboard/<org>/billing
  ```

- [ ] **Monitor storage growth**
  ```bash
  # Check R2 bucket size
  # Cloudflare Dashboard → R2 → bemidb-data → Metrics
  ```

#### Monthly

- [ ] **Review and optimize sync schedules**
  - Identify tables that rarely change → reduce frequency
  - Identify tables with high query volume → increase frequency

- [ ] **Check Neon autosuspend metrics**
  ```bash
  # Neon Console → Project → Metrics
  # Verify autosuspend working (cost optimization)
  ```

- [ ] **Rotate credentials** (quarterly recommended)
  - Regenerate Fly API token
  - Rotate R2 API keys
  - Update database passwords

#### Quarterly

- [ ] **Review cost trends**
  - Google Cloud Scheduler: Should be stable ($0.30/month)
  - Fly.io Syncer: Check for unexpected increases
  - R2 Storage: Plan for growth

- [ ] **Update BemiDB version**
  ```bash
  # Check for new releases
  # https://github.com/BemiHQ/BemiDB/releases

  # Update fly.toml and fly.syncer.toml
  [build]
    image = "ghcr.io/bemihq/bemidb:NEW_VERSION"

  # Redeploy
  flyctl deploy -a bemidb-server
  flyctl deploy -a bemidb-syncer -c fly.syncer.toml
  ```

---

### Backup Strategy

#### Catalog Database (Neon)

**Automatic backups:**
- Neon provides automatic daily backups (7-day retention)
- Point-in-time restore available

**Manual backup:**
```bash
# Export catalog
pg_dump "$CATALOG_DATABASE_URL" > catalog-backup-$(date +%Y%m%d).sql

# Upload to R2 for safekeeping
aws s3 cp catalog-backup-$(date +%Y%m%d).sql \
  s3://$AWS_S3_BUCKET/backups/ \
  --endpoint-url $AWS_S3_ENDPOINT
```

#### R2 Storage

**Automatic durability:**
- R2 has 99.999999999% (11 9's) durability
- No additional backups needed for data durability

**Optional: Cross-region replication**
```bash
# Replicate to another R2 bucket (different region/account)
aws s3 sync s3://$AWS_S3_BUCKET s3://bemidb-backup-bucket \
  --endpoint-url $AWS_S3_ENDPOINT
```

#### Configuration Backup

**Backup Cloud Scheduler jobs:**
```bash
# Export all jobs
gcloud scheduler jobs list --location=us-east1 --format=json > scheduler-jobs-backup.json

# Commit to Git
git add scheduler-jobs-backup.json
git commit -m "Backup Cloud Scheduler configuration"
```

**Backup Fly.io configuration:**
```bash
# Backup secrets list (not values, just names)
flyctl secrets list -a bemidb-server > secrets-server.txt
flyctl secrets list -a bemidb-syncer > secrets-syncer.txt

# Backup fly.toml files (already in Git)
git add fly.toml fly.syncer.toml
git commit -m "Backup Fly.io configuration"
```

---

### Disaster Recovery

#### Scenario 1: Catalog Database Lost

**Recovery steps:**
```bash
# 1. Create new Neon database
neonctl databases create catalog-new

# 2. Get new connection string
NEW_CATALOG_URL="postgresql://...neon.tech/catalog-new?sslmode=require"

# 3. Update Fly.io secrets
flyctl secrets set CATALOG_DATABASE_URL="$NEW_CATALOG_URL" -a bemidb-server
flyctl secrets set CATALOG_DATABASE_URL="$NEW_CATALOG_URL" -a bemidb-syncer

# 4. Re-run all syncs to rebuild catalog
gcloud scheduler jobs run bemidb-hourly-sync --location=us-east1
gcloud scheduler jobs run bemidb-daily-sync --location=us-east1

# 5. Verify catalog rebuilt
psql "$NEW_CATALOG_URL" -c "SELECT count(*) FROM iceberg_tables"
```

**Recovery time:** ~1-2 hours (depending on data size)

#### Scenario 2: R2 Bucket Deleted

**Recovery steps:**
```bash
# 1. Create new R2 bucket
# Via Cloudflare Dashboard → R2 → Create bucket

# 2. Update Fly.io secrets (if bucket name changed)
flyctl secrets set AWS_S3_BUCKET="new-bucket-name" -a bemidb-server
flyctl secrets set AWS_S3_BUCKET="new-bucket-name" -a bemidb-syncer

# 3. Re-run all syncs
# (Same as Scenario 1, step 4)

# 4. Verify data in new bucket
aws s3 ls s3://new-bucket-name/iceberg/ \
  --recursive \
  --endpoint-url $AWS_S3_ENDPOINT
```

**Recovery time:** ~1-2 hours

#### Scenario 3: Fly.io App Deleted

**Recovery steps:**
```bash
# 1. Recreate Fly.io apps
flyctl launch --no-deploy --name bemidb-server
flyctl launch --no-deploy --name bemidb-syncer -c fly.syncer.toml

# 2. Set secrets (same as original deployment)
# (See Infrastructure Setup steps)

# 3. Deploy
flyctl deploy -a bemidb-server
flyctl deploy -a bemidb-syncer -c fly.syncer.toml

# 4. Update Cloud Scheduler with new machine ID
flyctl machines list -a bemidb-syncer  # Get new machine ID
# Update scheduler jobs (see Google Cloud Scheduler Setup)
```

**Recovery time:** ~30 minutes

---

## Summary

### What You've Deployed

✅ **BemiDB Server** on Fly.io
- Postgres-compatible query interface
- Always-on, serves analytical queries
- Connected to Neon catalog and R2 storage

✅ **BemiDB Syncer** on Fly.io
- Pay-per-second machine
- Starts on-demand via API
- Syncs different tables on different schedules

✅ **Neon PostgreSQL Catalog**
- Stores Iceberg table metadata
- Autoscaling, autosuspend enabled
- Small footprint (~100 MB)

✅ **Cloudflare R2 Storage**
- Parquet files in columnar format
- 4x compression
- Zero egress fees

✅ **Google Cloud Scheduler**
- 99.9% SLA for reliable timing
- Multiple schedules for different tables
- Parameterized API calls

### What You Can Do

- ✅ **Query data** via standard Postgres clients (psql, Metabase, Grafana, etc.)
- ✅ **Sync different tables** on different schedules (hourly, daily, weekly)
- ✅ **Run complex analytics** 2000x faster than regular Postgres
- ✅ **Scale cheaply** with R2 storage and pay-per-second syncer
- ✅ **Monitor reliably** with Cloud Logging and Fly.io metrics

### Monthly Cost

**Total: ~$37-62/month**

Breakdown:
- Fly.io Server: $30/month (always-on)
- Fly.io Syncer: $5-25/month (pay-per-second, varies by frequency)
- Neon Catalog: $5/month (or free tier)
- Cloudflare R2: $1.50/month (100 GB storage)
- Cloud Scheduler: $0.30/month (3 jobs)

**vs Alternatives:**
- Snowflake + Fivetran: $300-1,000/month
- **BemiDB: 10-25x cheaper** 💰

---

## Next Steps

1. **Connect BI tools** to `postgresql://bemidb-server.fly.dev:5432/bemidb`
2. **Optimize sync schedules** based on actual query patterns
3. **Set up alerting** for failed syncs (Cloud Monitoring)
4. **Monitor costs** weekly for first month
5. **Document table sync frequencies** for team

---

## Support

**Issues or questions?**
- File an issue: https://github.com/BemiHQ/BemiDB/issues
- BemiDB docs: https://github.com/BemiHQ/BemiDB
- Fly.io docs: https://fly.io/docs
- Google Cloud Scheduler docs: https://cloud.google.com/scheduler/docs

---

**Document version:** 1.0
**Last updated:** 2025-01-15
**Maintained for:** Claude Code instances and BemiDB operators using Cloud Scheduler + Fly.io
