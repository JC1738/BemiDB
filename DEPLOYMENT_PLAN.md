# BemiDB Production Deployment Plan

**Infrastructure Stack:**
- **Server**: Fly.io (Postgres wire protocol)
- **Catalog**: Neon PostgreSQL (managed Postgres)
- **Storage**: Cloudflare R2 (S3-compatible)
- **Cron**: Multiple options analyzed below

**Code Modifications:** DeleteOldTables() removed to support independent table syncing on different schedules.

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Code Modifications](#code-modifications)
3. [Infrastructure Setup](#infrastructure-setup)
4. [Cron Service Comparison](#cron-service-comparison)
5. [Deployment Steps](#deployment-steps)
6. [Configuration Examples](#configuration-examples)
7. [Cost Estimates](#cost-estimates)
8. [Monitoring & Operations](#monitoring--operations)
9. [Troubleshooting](#troubleshooting)

---

## Prerequisites

### Required Accounts
- [ ] Fly.io account (https://fly.io/signup)
- [ ] Neon account (https://neon.tech)
- [ ] Cloudflare account with R2 access (https://dash.cloudflare.com)
- [ ] GitHub account (optional, for GitHub Actions cron)
- [ ] AWS account (optional, for EventBridge cron)

### Required Tools
```bash
# Install flyctl
curl -L https://fly.io/install.sh | sh

# Install neon CLI (optional)
npm install -g neonctl

# Install AWS CLI (if using EventBridge)
pip install awscli

# Install gcloud CLI (if using Cloud Scheduler)
curl https://sdk.cloud.google.com | bash
```

---

## Code Modifications

### Change Made: Remove DeleteOldTables()

**File**: `src/syncer-postgres/lib/syncer_full_refresh.go`

**What was changed:**
```go
// BEFORE (line 35):
syncer.Utils.DeleteOldTables(icebergTableNames)

// AFTER:
// NOTE: DeleteOldTables() has been removed to support independent table syncing.
// This allows running multiple syncer instances with different SOURCE_POSTGRES_INCLUDE_TABLES
// on different schedules without deleting tables from previous syncs.
// To clean up old/renamed tables, manually drop them: DROP TABLE schema.table_name
// syncer.Utils.DeleteOldTables(icebergTableNames)
```

**Impact:**
- ✅ Enables syncing different tables on different schedules
- ✅ Tables not in current sync are preserved (not deleted)
- ⚠️ Manual cleanup required for renamed/removed tables

**Example use case:**
```bash
# Hourly: Sync fast-changing tables
docker run -e SOURCE_POSTGRES_INCLUDE_TABLES=public.users,public.sessions ...

# Daily: Sync slow-changing tables
docker run -e SOURCE_POSTGRES_INCLUDE_TABLES=public.analytics,public.reports ...

# Result: Both sets of tables coexist in BemiDB
```

---

## Infrastructure Setup

### 1. Neon PostgreSQL (Catalog Database)

#### Create Neon Project
```bash
# Via Neon Console (https://console.neon.tech)
# 1. Create new project: "bemidb-catalog"
# 2. Region: Choose closest to your Fly.io region
# 3. Postgres version: 16 (latest)
# 4. Compute size: 0.25 vCPU (sufficient for catalog)

# Or via CLI:
neonctl projects create --name bemidb-catalog --region aws-us-east-1
neonctl databases create catalog
```

#### Get Connection String
```bash
# From Neon Console → Connection Details
# Format: postgresql://[user]:[password]@[endpoint]/catalog?sslmode=require

# Example:
CATALOG_DATABASE_URL="postgresql://user:password@ep-cool-darkness-123456.us-east-1.aws.neon.tech/catalog?sslmode=require"
```

#### Initialize Catalog Schema
The catalog schema is auto-created by BemiDB on first syncer run, but you can verify:
```bash
psql $CATALOG_DATABASE_URL -c "
CREATE TABLE IF NOT EXISTS iceberg_tables (
  table_namespace VARCHAR(255) NOT NULL,
  table_name VARCHAR(255) NOT NULL,
  metadata_location VARCHAR(1000),
  columns JSONB
);
CREATE UNIQUE INDEX IF NOT EXISTS iceberg_tables_pkey
  ON iceberg_tables(table_namespace, table_name);
"
```

**Neon Configuration:**
- **Autoscaling**: Enable (0.25-0.5 vCPU range)
- **Autosuspend**: 5 minutes (catalog has bursty usage)
- **Storage**: 1GB sufficient (catalog is small)
- **Pooling**: Enable connection pooling (pgbouncer mode)

---

### 2. Cloudflare R2 (S3-Compatible Storage)

#### Create R2 Bucket
```bash
# Via Cloudflare Dashboard (https://dash.cloudflare.com)
# 1. Navigate to R2 → Create bucket
# 2. Bucket name: "bemidb-data"
# 3. Location: Automatic (R2 is globally distributed)
```

#### Create R2 API Token
```bash
# Dashboard → R2 → Manage R2 API Tokens
# 1. Create API token
# 2. Permissions: Object Read & Write
# 3. Scope: Specific bucket (bemidb-data)
# 4. Save Access Key ID and Secret Access Key
```

#### Get R2 Configuration
```bash
# From R2 bucket settings:
AWS_S3_BUCKET="bemidb-data"
AWS_ACCESS_KEY_ID="<your-r2-access-key-id>"
AWS_SECRET_ACCESS_KEY="<your-r2-secret-access-key>"

# R2 endpoint format:
# https://<account-id>.r2.cloudflarestorage.com
AWS_S3_ENDPOINT="https://1234567890abcdef.r2.cloudflarestorage.com"

# R2 uses "auto" region (global distribution)
AWS_REGION="auto"
```

**R2 Configuration Notes:**
- R2 is S3-compatible (no AWS account needed)
- Global distribution (no region selection)
- Zero egress fees (unlike S3)
- Pricing: $0.015/GB/month storage

---

### 3. Fly.io (BemiDB Server)

#### Install and Authenticate
```bash
# Install flyctl
curl -L https://fly.io/install.sh | sh

# Login
flyctl auth login

# Create fly.toml configuration
flyctl launch --no-deploy
```

#### Create fly.toml
Create `/home/jc/git/BemiDB/fly.toml`:
```toml
app = "bemidb-server"
primary_region = "iad" # us-east, close to Neon

[build]
  image = "ghcr.io/bemihq/bemidb:latest"

[env]
  BEMIDB_PORT = "54321"
  BEMIDB_HOST = "0.0.0.0"
  BEMIDB_DATABASE = "bemidb"
  BEMIDB_LOG_LEVEL = "INFO"
  BEMIDB_DISABLE_ANONYMOUS_ANALYTICS = "true"
  AWS_REGION = "auto"  # R2 uses "auto"

[[services]]
  internal_port = 54321
  protocol = "tcp"

  [[services.ports]]
    port = 5432  # External Postgres port
    handlers = ["tls"]

  [[services.ports]]
    port = 54321
    handlers = []

[http_service]
  internal_port = 54321
  force_https = false
  auto_stop_machines = "suspend"
  auto_start_machines = true
  min_machines_running = 1
  processes = ["app"]

[[vm]]
  size = "shared-cpu-2x"  # 2 vCPU, 4GB RAM (DuckDB needs 3GB)
  memory = "4gb"
```

#### Set Fly.io Secrets
```bash
# Set sensitive environment variables as secrets
flyctl secrets set \
  CATALOG_DATABASE_URL="postgresql://user:pass@ep-xxx.neon.tech/catalog?sslmode=require" \
  AWS_S3_BUCKET="bemidb-data" \
  AWS_ACCESS_KEY_ID="<r2-access-key>" \
  AWS_SECRET_ACCESS_KEY="<r2-secret-key>" \
  AWS_S3_ENDPOINT="https://xxx.r2.cloudflarestorage.com"
```

#### Deploy BemiDB Server
```bash
# Deploy
flyctl deploy

# Check status
flyctl status

# View logs
flyctl logs

# Get connection string
flyctl info
# Connect via: postgresql://<app-name>.fly.dev:5432/bemidb
```

#### Test Connection
```bash
# Test from local machine
psql postgresql://<app-name>.fly.dev:5432/bemidb -c "SELECT 1"

# List tables (should be empty initially)
psql postgresql://<app-name>.fly.dev:5432/bemidb -c \
  "SELECT table_schema, table_name FROM information_schema.tables"
```

---

## Cron Service Comparison

### Overview of Options

| Service | Cost (monthly) | Setup Complexity | Reliability | Best For |
|---------|---------------|------------------|-------------|----------|
| **GitHub Actions** | Free (2,000 min/month) | Low | High | Open-source projects, simple schedules |
| **Fly.io Machines** | $0.0003/sec (~$0.78/hr run) | Medium | High | Integrated deployment, same infrastructure |
| **AWS EventBridge** | $1/million invocations | Medium | Very High | Enterprise, AWS ecosystem |
| **Google Cloud Scheduler** | $0.10/job/month + runtime | Medium | Very High | GCP ecosystem, simple pricing |
| **Render Cron Jobs** | Free tier available | Low | High | Simple setup, integrated |
| **Temporal.io** | Free (self-hosted) | High | Very High | Complex workflows, retries |

---

### Option 1: GitHub Actions (Recommended for Simple Use Cases) ⭐

**Pros:**
- ✅ Free tier: 2,000 minutes/month (enough for ~40 hourly jobs)
- ✅ Simple YAML configuration
- ✅ Built-in secrets management
- ✅ Integrated with Git repository
- ✅ Good logging and monitoring
- ✅ No additional infrastructure

**Cons:**
- ❌ Minimum schedule: every 5 minutes
- ❌ Not guaranteed exact timing (can be delayed)
- ❌ Requires public or private GitHub repo

**Setup:**

Create `.github/workflows/sync-hourly.yml`:
```yaml
name: BemiDB Hourly Sync

on:
  schedule:
    # Runs every hour at minute 0
    - cron: '0 * * * *'
  workflow_dispatch: # Manual trigger

jobs:
  sync-postgres:
    runs-on: ubuntu-latest
    steps:
      - name: Sync fast-changing tables
        run: |
          docker run --rm \
            -e SOURCE_POSTGRES_DATABASE_URL="${{ secrets.SOURCE_POSTGRES_URL }}" \
            -e SOURCE_POSTGRES_INCLUDE_TABLES="public.users,public.sessions,public.orders" \
            -e DESTINATION_SCHEMA_NAME="postgres" \
            -e CATALOG_DATABASE_URL="${{ secrets.CATALOG_DATABASE_URL }}" \
            -e AWS_REGION="${{ secrets.AWS_REGION }}" \
            -e AWS_S3_BUCKET="${{ secrets.AWS_S3_BUCKET }}" \
            -e AWS_ACCESS_KEY_ID="${{ secrets.AWS_ACCESS_KEY_ID }}" \
            -e AWS_SECRET_ACCESS_KEY="${{ secrets.AWS_SECRET_ACCESS_KEY }}" \
            -e AWS_S3_ENDPOINT="${{ secrets.AWS_S3_ENDPOINT }}" \
            ghcr.io/bemihq/bemidb:latest syncer-postgres
```

Create `.github/workflows/sync-daily.yml`:
```yaml
name: BemiDB Daily Sync

on:
  schedule:
    # Runs daily at 2 AM UTC
    - cron: '0 2 * * *'
  workflow_dispatch:

jobs:
  sync-postgres:
    runs-on: ubuntu-latest
    steps:
      - name: Sync slow-changing tables
        run: |
          docker run --rm \
            -e SOURCE_POSTGRES_DATABASE_URL="${{ secrets.SOURCE_POSTGRES_URL }}" \
            -e SOURCE_POSTGRES_INCLUDE_TABLES="public.analytics,public.reports,public.archives" \
            -e DESTINATION_SCHEMA_NAME="postgres" \
            -e CATALOG_DATABASE_URL="${{ secrets.CATALOG_DATABASE_URL }}" \
            -e AWS_REGION="${{ secrets.AWS_REGION }}" \
            -e AWS_S3_BUCKET="${{ secrets.AWS_S3_BUCKET }}" \
            -e AWS_ACCESS_KEY_ID="${{ secrets.AWS_ACCESS_KEY_ID }}" \
            -e AWS_SECRET_ACCESS_KEY="${{ secrets.AWS_SECRET_ACCESS_KEY }}" \
            -e AWS_S3_ENDPOINT="${{ secrets.AWS_S3_ENDPOINT }}" \
            ghcr.io/bemihq/bemidb:latest syncer-postgres
```

**Configure secrets:**
```bash
# Via GitHub Settings → Secrets and variables → Actions
# Add the following secrets:
- SOURCE_POSTGRES_URL
- CATALOG_DATABASE_URL
- AWS_REGION
- AWS_S3_BUCKET
- AWS_ACCESS_KEY_ID
- AWS_SECRET_ACCESS_KEY
- AWS_S3_ENDPOINT
```

**Cost:** Free for public repos, $4/month for private repos (includes other features)

---

### Option 2: Fly.io Machines (Recommended for Production) ⭐⭐

**Pros:**
- ✅ Integrated with Fly.io deployment
- ✅ Same infrastructure as server
- ✅ Pay only for execution time (billed per second)
- ✅ Fast startup (machines start in ~2 seconds)
- ✅ Shared secrets with server
- ✅ Reliable scheduling

**Cons:**
- ❌ Requires external scheduler (GitHub Actions, cron-job.org, etc.)
- ❌ More complex setup than GitHub Actions
- ⚠️ Need to trigger Fly machines via API

**Setup:**

Create `fly.syncer.toml`:
```toml
app = "bemidb-syncer"
primary_region = "iad"

[build]
  image = "ghcr.io/bemihq/bemidb:latest"

[env]
  DESTINATION_SCHEMA_NAME = "postgres"
  AWS_REGION = "auto"
  BEMIDB_LOG_LEVEL = "INFO"

[[vm]]
  size = "shared-cpu-1x"
  memory = "2gb"

[processes]
  syncer = "/app/bin/run.sh syncer-postgres"
```

Deploy syncer machine:
```bash
# Deploy syncer machine
flyctl deploy -c fly.syncer.toml

# Set secrets (same as server)
flyctl secrets set -a bemidb-syncer \
  SOURCE_POSTGRES_DATABASE_URL="..." \
  CATALOG_DATABASE_URL="..." \
  AWS_S3_BUCKET="..." \
  AWS_ACCESS_KEY_ID="..." \
  AWS_SECRET_ACCESS_KEY="..." \
  AWS_S3_ENDPOINT="..."

# Configure as machine (not service)
flyctl scale count 0 -a bemidb-syncer
```

Trigger via GitHub Actions:
```yaml
name: Trigger Fly.io Sync

on:
  schedule:
    - cron: '0 * * * *'
  workflow_dispatch:

jobs:
  trigger-sync:
    runs-on: ubuntu-latest
    steps:
      - name: Trigger Fly.io Machine
        env:
          FLY_API_TOKEN: ${{ secrets.FLY_API_TOKEN }}
        run: |
          curl -X POST \
            -H "Authorization: Bearer $FLY_API_TOKEN" \
            -H "Content-Type: application/json" \
            https://api.machines.dev/v1/apps/bemidb-syncer/machines/start
```

**Cost:** ~$0.0003/second = ~$1.08/hour of runtime
- Hourly sync (5 min each): ~$2.70/month
- Daily sync (10 min each): ~$0.90/month

---

### Option 3: AWS EventBridge + Lambda

**Pros:**
- ✅ Enterprise-grade reliability (99.99% SLA)
- ✅ Precise scheduling (minute-level accuracy)
- ✅ Rich event routing capabilities
- ✅ Integrated with AWS ecosystem

**Cons:**
- ❌ Requires AWS account
- ❌ More complex setup (IAM roles, Lambda, etc.)
- ❌ Lambda has 15-minute timeout (need ECS/Fargate for longer syncs)
- ⚠️ Cold starts can add latency

**Setup:**

Create Lambda function or ECS task that runs Docker container:
```python
# Lambda handler (Python)
import boto3
import os

def lambda_handler(event, context):
    ecs = boto3.client('ecs')

    response = ecs.run_task(
        cluster='bemidb-cluster',
        taskDefinition='bemidb-syncer',
        launchType='FARGATE',
        networkConfiguration={
            'awsvpcConfiguration': {
                'subnets': [os.environ['SUBNET_ID']],
                'assignPublicIp': 'ENABLED'
            }
        }
    )
    return response
```

EventBridge rule:
```bash
aws events put-rule \
  --name bemidb-hourly-sync \
  --schedule-expression "cron(0 * * * ? *)"

aws events put-targets \
  --rule bemidb-hourly-sync \
  --targets "Id"="1","Arn"="arn:aws:lambda:us-east-1:xxx:function:bemidb-syncer"
```

**Cost:**
- EventBridge: $1/million invocations (~$0.01/month for hourly)
- Lambda: Free tier covers small usage
- ECS Fargate: $0.04/vCPU-hour + $0.004/GB-hour

---

### Option 4: Google Cloud Scheduler

**Pros:**
- ✅ Simple, purpose-built for cron jobs
- ✅ Reliable (99.9% SLA)
- ✅ Transparent pricing ($0.10/job/month)
- ✅ Integrates with Cloud Run (serverless containers)

**Cons:**
- ❌ Requires GCP account
- ⚠️ Need Cloud Run to execute Docker containers

**Setup:**
```bash
# Create Cloud Run job
gcloud run jobs create bemidb-syncer \
  --image ghcr.io/bemihq/bemidb:latest \
  --args syncer-postgres \
  --set-env-vars "DESTINATION_SCHEMA_NAME=postgres,AWS_REGION=auto" \
  --set-secrets "SOURCE_POSTGRES_DATABASE_URL=source-db:latest,..." \
  --region us-east1

# Create scheduler
gcloud scheduler jobs create http bemidb-hourly \
  --schedule "0 * * * *" \
  --uri "https://us-east1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/PROJECT/jobs/bemidb-syncer:run" \
  --oauth-service-account-email "PROJECT@appspot.gserviceaccount.com"
```

**Cost:**
- Cloud Scheduler: $0.10/job/month
- Cloud Run: $0.00002400/vCPU-second + $0.00000250/GiB-second
- Estimate: ~$1-3/month for hourly + daily jobs

---

### Option 5: Render Cron Jobs

**Pros:**
- ✅ Extremely simple setup (UI-based)
- ✅ Free tier available
- ✅ Integrated with Render deployments
- ✅ Good for straightforward schedules

**Cons:**
- ❌ Limited to standard cron syntax
- ⚠️ Free tier has resource limits

**Setup:**
```yaml
# render.yaml
services:
  - type: web
    name: bemidb-server
    env: docker
    image:
      url: ghcr.io/bemihq/bemidb:latest
    plan: starter
    envVars:
      - key: BEMIDB_PORT
        value: 54321
      # ... other env vars

  - type: cron
    name: bemidb-hourly-sync
    env: docker
    image:
      url: ghcr.io/bemihq/bemidb:latest
    schedule: "0 * * * *"
    plan: starter
    envVars:
      - key: DESTINATION_SCHEMA_NAME
        value: postgres
      # ... other env vars
```

**Cost:**
- Free tier: 750 hours/month
- Paid: $7/month (starter) for dedicated resources

---

### Recommended Choice: GitHub Actions + Fly.io Machines

**For most users:**
```
GitHub Actions (scheduling) → Fly.io Machines (execution)
```

**Why:**
1. **GitHub Actions** handles scheduling (free, reliable, simple YAML)
2. **Fly.io Machines** handles execution (pay-per-second, fast, same infrastructure as server)
3. Best balance of cost, simplicity, and reliability
4. Both tools are developer-friendly

**For enterprise:**
```
AWS EventBridge → ECS Fargate
```

**Why:**
- Enterprise SLA requirements
- Existing AWS infrastructure
- Advanced event routing needs
- Compliance requirements

---

## Deployment Steps

### Step 1: Set Up Infrastructure (30 minutes)

```bash
# 1. Create Neon database
# Via https://console.neon.tech
# Save connection string

# 2. Create R2 bucket
# Via https://dash.cloudflare.com → R2
# Save API credentials

# 3. Deploy Fly.io server
cd /home/jc/git/BemiDB
flyctl launch --no-deploy
# Edit fly.toml (see template above)
flyctl secrets set CATALOG_DATABASE_URL="..." AWS_S3_BUCKET="..." # etc
flyctl deploy

# 4. Verify server is running
flyctl status
flyctl logs
```

### Step 2: Configure GitHub Actions (10 minutes)

```bash
# 1. Create workflows directory
mkdir -p .github/workflows

# 2. Create hourly sync workflow
cat > .github/workflows/sync-hourly.yml << 'EOF'
# (Use template from Option 1 above)
EOF

# 3. Create daily sync workflow
cat > .github/workflows/sync-daily.yml << 'EOF'
# (Use template from Option 1 above)
EOF

# 4. Push to GitHub
git add .github/
git commit -m "Add GitHub Actions sync workflows"
git push

# 5. Configure secrets in GitHub
# Go to Settings → Secrets → New repository secret
# Add all required secrets
```

### Step 3: Run Initial Sync (5 minutes)

```bash
# Trigger manual sync via GitHub Actions
# Go to Actions → Sync Hourly → Run workflow

# Or run locally to test:
docker run --rm \
  -e SOURCE_POSTGRES_DATABASE_URL="postgresql://user:pass@host:5432/db" \
  -e SOURCE_POSTGRES_INCLUDE_TABLES="public.users,public.orders" \
  -e DESTINATION_SCHEMA_NAME="postgres" \
  -e CATALOG_DATABASE_URL="postgresql://..." \
  -e AWS_REGION="auto" \
  -e AWS_S3_BUCKET="bemidb-data" \
  -e AWS_ACCESS_KEY_ID="..." \
  -e AWS_SECRET_ACCESS_KEY="..." \
  -e AWS_S3_ENDPOINT="https://xxx.r2.cloudflarestorage.com" \
  ghcr.io/bemihq/bemidb:latest syncer-postgres
```

### Step 4: Verify Deployment (5 minutes)

```bash
# 1. Check catalog database has entries
psql $CATALOG_DATABASE_URL -c "SELECT table_namespace, table_name FROM iceberg_tables"

# 2. Check R2 bucket has data
# Via Cloudflare Dashboard → R2 → bemidb-data
# Should see: iceberg/postgres/[table-name]/ directories with .parquet files

# 3. Query BemiDB server
psql postgresql://<app-name>.fly.dev:5432/bemidb -c \
  "SELECT table_schema, table_name FROM information_schema.tables"

# 4. Run test query
psql postgresql://<app-name>.fly.dev:5432/bemidb -c \
  "SELECT COUNT(*) FROM postgres.users"
```

### Step 5: Monitor First Scheduled Run (varies)

```bash
# Watch GitHub Actions
# Go to Actions tab in GitHub → Wait for next scheduled run

# Monitor Fly.io logs during sync
flyctl logs --app bemidb-server

# Check for errors in GitHub Actions logs
```

---

## Configuration Examples

### Environment Variables Reference

**Required for Server:**
```bash
CATALOG_DATABASE_URL="postgresql://user:pass@ep-xxx.neon.tech/catalog?sslmode=require"
AWS_REGION="auto"
AWS_S3_BUCKET="bemidb-data"
AWS_ACCESS_KEY_ID="<r2-access-key>"
AWS_SECRET_ACCESS_KEY="<r2-secret-key>"
AWS_S3_ENDPOINT="https://xxx.r2.cloudflarestorage.com"
```

**Required for Syncer:**
```bash
# All server variables above, plus:
SOURCE_POSTGRES_DATABASE_URL="postgresql://user:pass@host:5432/sourcedb"
DESTINATION_SCHEMA_NAME="postgres"

# Optional syncer variables:
SOURCE_POSTGRES_INCLUDE_TABLES="public.table1,public.table2"
SOURCE_POSTGRES_EXCLUDE_TABLES="public.audit_log"
BEMIDB_LOG_LEVEL="INFO"
```

### Multiple Sync Schedules Example

**Hourly sync** (fast-changing tables):
```yaml
# .github/workflows/sync-hourly.yml
on:
  schedule:
    - cron: '0 * * * *'  # Every hour

env:
  INCLUDE_TABLES: "public.users,public.sessions,public.orders,public.events"
```

**Every 6 hours** (medium-changing tables):
```yaml
# .github/workflows/sync-6hourly.yml
on:
  schedule:
    - cron: '0 */6 * * *'  # Every 6 hours

env:
  INCLUDE_TABLES: "public.products,public.inventory,public.customers"
```

**Daily sync** (slow-changing tables):
```yaml
# .github/workflows/sync-daily.yml
on:
  schedule:
    - cron: '0 2 * * *'  # Daily at 2 AM UTC

env:
  INCLUDE_TABLES: "public.analytics,public.reports,public.archives,public.backups"
```

**Result:** All tables coexist in BemiDB, synced at appropriate frequencies.

---

## Cost Estimates

### Monthly Infrastructure Costs

| Service | Usage | Cost |
|---------|-------|------|
| **Fly.io Server** | 1x shared-cpu-2x (4GB) 24/7 | ~$30/month |
| **Neon Catalog** | 0.25 vCPU, 1GB storage, autosuspend | ~$5/month (free tier available) |
| **Cloudflare R2** | 100GB storage, 1TB egress (free) | ~$1.50/month |
| **GitHub Actions** | Hourly + daily syncs (public repo) | Free |
| **TOTAL** | | **~$36.50/month** |

### Cost Optimization Options

**Free Tier Option:**
- Neon: Free tier (0.5 GB storage, autosuspend)
- Fly.io: $5 free credit/month (covers ~17% of server)
- R2: 10GB free/month
- GitHub Actions: Free for public repos
- **Actual cost: ~$25-30/month**

**Scale-Up Scenario (1TB data):**
- Fly.io Server: $30/month (same)
- Neon: $5/month (catalog stays small)
- R2: ~$15/month (1TB storage)
- GitHub Actions: Free
- **Total: ~$50/month**

**Comparison with Alternatives:**
- Snowflake: ~$200-500/month minimum
- Fivetran + Snowflake: ~$300-1000/month
- Managed Postgres (1TB): ~$200+/month
- **BemiDB: ~$36-50/month** 💰

---

## Monitoring & Operations

### Logging

**Fly.io server logs:**
```bash
# Real-time logs
flyctl logs -a bemidb-server

# Search logs
flyctl logs -a bemidb-server --search "ERROR"

# Export logs to file
flyctl logs -a bemidb-server > bemidb.log
```

**GitHub Actions logs:**
```bash
# View in GitHub UI: Actions → Workflow → Run

# Or via CLI:
gh run list --workflow sync-hourly.yml
gh run view <run-id> --log
```

**Neon metrics:**
```bash
# Via Neon Console → Metrics
# Shows: CPU usage, connections, storage
```

### Monitoring Checklist

- [ ] Set up Fly.io health checks
- [ ] Configure GitHub Actions failure notifications
- [ ] Monitor R2 storage growth
- [ ] Track sync duration trends
- [ ] Set up alerts for failed syncs
- [ ] Monitor Neon connection count

### Alerting

**GitHub Actions email notifications:**
- Automatically enabled for workflow failures
- Configure in Settings → Notifications

**Fly.io monitoring:**
```toml
# Add to fly.toml
[http_service]
  ...
  [[http_service.checks]]
    interval = "30s"
    timeout = "5s"
    method = "GET"
    path = "/"
```

**Custom alerting script:**
```bash
# .github/workflows/alert-on-failure.yml
on:
  workflow_run:
    workflows: ["BemiDB Hourly Sync"]
    types: [completed]

jobs:
  alert:
    if: ${{ github.event.workflow_run.conclusion == 'failure' }}
    runs-on: ubuntu-latest
    steps:
      - name: Send alert
        run: |
          curl -X POST https://hooks.slack.com/services/YOUR/WEBHOOK \
            -d '{"text":"BemiDB sync failed!"}'
```

### Backup Strategy

**Catalog database:**
```bash
# Neon has automatic daily backups (7-day retention)
# Manual backup:
pg_dump $CATALOG_DATABASE_URL > catalog-backup-$(date +%Y%m%d).sql
```

**R2 data:**
```bash
# R2 is durable (11 9's durability)
# Optional: Enable R2 versioning in Cloudflare Dashboard
# Or sync to another bucket:
aws s3 sync s3://bemidb-data s3://bemidb-backup --endpoint-url $AWS_S3_ENDPOINT
```

**Configuration backup:**
```bash
# Backup Fly.io secrets
flyctl secrets list -a bemidb-server > secrets-backup.txt

# Backup GitHub secrets (manual export from UI)
```

---

## Troubleshooting

### Common Issues

#### Issue: "ERROR: could not connect to catalog database"

**Diagnosis:**
```bash
# Test catalog connection
psql $CATALOG_DATABASE_URL -c "SELECT 1"
```

**Solutions:**
- Check Neon project is not suspended (auto-suspends after inactivity)
- Verify connection string has `?sslmode=require`
- Check firewall rules allow Fly.io IP ranges
- Verify credentials are correct

---

#### Issue: "ERROR: S3 operation failed: Access Denied"

**Diagnosis:**
```bash
# Test R2 access
aws s3 ls s3://$AWS_S3_BUCKET --endpoint-url $AWS_S3_ENDPOINT
```

**Solutions:**
- Verify R2 API token has read/write permissions
- Check bucket name is correct
- Ensure `AWS_S3_ENDPOINT` includes `https://` prefix
- Verify token is not expired

---

#### Issue: "ERROR: table not found in BemiDB"

**Diagnosis:**
```bash
# Check catalog
psql $CATALOG_DATABASE_URL -c "SELECT * FROM iceberg_tables WHERE table_name = 'users'"

# Check R2
aws s3 ls s3://$AWS_S3_BUCKET/iceberg/postgres/ --endpoint-url $AWS_S3_ENDPOINT
```

**Solutions:**
- Verify sync completed successfully (check logs)
- Ensure table was included in `SOURCE_POSTGRES_INCLUDE_TABLES`
- Check server can read from R2 (test credentials)
- Restart BemiDB server to reload catalog

---

#### Issue: GitHub Actions sync times out

**Diagnosis:**
```bash
# Check workflow run time in Actions tab
# Default timeout: 6 hours
```

**Solutions:**
- Reduce table size (use `EXCLUDE_TABLES` for large audit tables)
- Split into multiple workflows
- Increase timeout in workflow YAML:
  ```yaml
  jobs:
    sync:
      timeout-minutes: 60  # Default is 360
  ```

---

#### Issue: Sync performance is slow

**Diagnosis:**
```bash
# Check sync logs for bottlenecks
# Look for: "Copied X rows" time

# Test source database performance
time psql $SOURCE_POSTGRES_DATABASE_URL -c "COPY (SELECT * FROM large_table) TO STDOUT"
```

**Solutions:**
- Source database slow: Add indexes, optimize queries
- Network slow: Use source closer to BemiDB region
- R2 upload slow: Check batch size (default 100MB Parquet files is optimal)
- DuckDB memory: Increase Fly.io VM size

---

#### Issue: Catalog database running out of connections

**Diagnosis:**
```bash
# Check Neon connection count
psql $CATALOG_DATABASE_URL -c "SELECT count(*) FROM pg_stat_activity"
```

**Solutions:**
- Enable Neon connection pooling (pgbouncer mode)
- Reduce concurrent syncers
- Ensure syncer processes close connections (they should auto-close)

---

#### Issue: R2 storage costs growing unexpectedly

**Diagnosis:**
```bash
# Check R2 storage usage
# Via Cloudflare Dashboard → R2 → bemidb-data → Metrics

# List old snapshots
aws s3 ls s3://$AWS_S3_BUCKET/iceberg/postgres/users/metadata/ \
  --endpoint-url $AWS_S3_ENDPOINT
```

**Solutions:**
- Old Iceberg snapshots accumulating (each sync creates new snapshot)
- Run table compaction (not yet implemented in BemiDB, manual cleanup needed)
- Delete old snapshots manually:
  ```bash
  # List metadata files
  aws s3 ls s3://$AWS_S3_BUCKET/iceberg/postgres/users/metadata/ \
    --endpoint-url $AWS_S3_ENDPOINT

  # Delete old files (keep latest 3 versions)
  # Requires manual cleanup or custom script
  ```

---

### Debug Mode

**Enable TRACE logging:**
```bash
# In Fly.io
flyctl secrets set BEMIDB_LOG_LEVEL=TRACE -a bemidb-server

# In GitHub Actions
env:
  BEMIDB_LOG_LEVEL: "TRACE"
```

**Profile memory usage:**
```bash
# TRACE mode enables pprof on port 6060
# Access via Fly.io proxy:
flyctl proxy 6060:6060 -a bemidb-server

# Then visit: http://localhost:6060/debug/pprof/
```

---

## Next Steps

### Post-Deployment

1. **Set up monitoring alerts** (GitHub Actions notifications, Fly.io health checks)
2. **Configure backup schedule** (Neon auto-backups, R2 versioning)
3. **Optimize sync schedules** based on data change patterns
4. **Document table sync frequencies** for team
5. **Set up BI tool connections** (Metabase, Grafana, etc.)

### Future Enhancements

1. **Implement incremental sync** (currently not supported)
2. **Add CDC support** (real-time syncing)
3. **Automate Iceberg compaction** (reduce storage costs)
4. **Add materialized views** (pre-aggregate analytics)
5. **Multi-region deployment** (for global users)

---

## Summary

**What you've deployed:**
- ✅ BemiDB server on Fly.io (Postgres-compatible query interface)
- ✅ Neon PostgreSQL catalog (metadata storage)
- ✅ Cloudflare R2 storage (Parquet data files)
- ✅ GitHub Actions cron (automated syncing)
- ✅ Code modification (independent table syncing support)

**What you can do:**
- Query data via Postgres wire protocol from any client
- Sync different tables on different schedules (hourly, daily, etc.)
- Scale storage cheaply with R2
- Analyze data 2000x faster than regular Postgres
- Connect BI tools (Metabase, Grafana, etc.)

**Monthly cost:** ~$36-50 (vs $300-1000 for Snowflake + Fivetran)

---

**Questions or issues?** Check troubleshooting section above or file an issue at https://github.com/BemiHQ/BemiDB/issues

---

**Document version:** 1.0
**Last updated:** 2025-01-XX
**Maintained for:** Claude Code instances and BemiDB operators
