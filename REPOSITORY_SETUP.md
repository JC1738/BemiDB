# BemiDB Repository Setup Guide

This document explains the two-repository architecture for deploying BemiDB with custom modifications.

## Why Two Repositories?

Clean separation of concerns:

1. **BemiDB Fork** - Your customized codebase
2. **BemiDB Deployment** - Infrastructure configuration

---

## Repository 1: BemiDB Fork

### Purpose
Contains the modified BemiDB source code with `DeleteOldTables()` removed to support independent table syncing.

### Location
- **Upstream**: `https://github.com/BemiHQ/BemiDB`
- **Your fork**: `https://github.com/YOUR-USERNAME/BemiDB`

### Key Files Modified
- `src/syncer-postgres/lib/syncer_full_refresh.go:39` - DeleteOldTables() commented out

### Additions
- `.github/workflows/docker-build.yml` - Automated Docker image building

### Output
- Docker image published to: `ghcr.io/YOUR-USERNAME/bemidb:latest`
- Tags: `latest`, `main`, `v1.7.0-custom.1`, etc.

### Maintenance
```bash
# Keep fork updated with upstream
git remote add upstream https://github.com/BemiHQ/BemiDB.git
git fetch upstream
git merge upstream/main

# Re-apply DeleteOldTables removal if needed
# Push to trigger new Docker build
git push origin main
```

---

## Repository 2: BemiDB Deployment

### Purpose
Infrastructure-as-code for deploying BemiDB to Fly.io and Google Cloud Scheduler.

### Location
- **Your repo**: `https://github.com/YOUR-USERNAME/bemidb-deployment`

### Structure
```
bemidb-deployment/
├── config/
│   ├── fly.toml              # Server (wire protocol)
│   ├── fly.syncer.toml       # Syncer (sync machine)
│   └── .env.sample           # Environment template
├── scripts/
│   ├── deploy-server.sh      # Deploy server to Fly.io
│   ├── deploy-syncer.sh      # Deploy syncer to Fly.io
│   └── setup-cloud-scheduler.sh  # Create cron jobs
├── docs/
│   └── DEPLOYMENT.md         # Full deployment guide
└── README.md
```

### Key Configuration

**config/fly.toml** (Server):
```toml
[build]
  image = "ghcr.io/YOUR-USERNAME/bemidb:latest"  # Your custom image
```

**config/fly.syncer.toml** (Syncer):
```toml
[build]
  image = "ghcr.io/YOUR-USERNAME/bemidb:latest"  # Your custom image
```

### Usage
```bash
# Deploy everything
cd bemidb-deployment

# Setup environment
cp config/.env.sample .env
# Edit .env with your credentials

# Deploy
./scripts/deploy-server.sh
./scripts/deploy-syncer.sh
./scripts/setup-cloud-scheduler.sh
```

---

## Complete Workflow

### Initial Setup (One-Time)

**1. Fork BemiDB**
```bash
# On GitHub: Fork BemiHQ/BemiDB to YOUR-USERNAME/BemiDB
git clone https://github.com/YOUR-USERNAME/BemiDB.git
cd BemiDB

# Verify modification
grep "DeleteOldTables" src/syncer-postgres/lib/syncer_full_refresh.go
# Should show commented code
```

**2. Add GitHub Actions**
```bash
# Create auto-build workflow
mkdir -p .github/workflows
# (Copy docker-build.yml from DEPLOYMENT_PLAN_CLOUD_SCHEDULER.md)

git add .github/workflows/
git commit -m "Add Docker auto-build"
git push origin main
```

**3. Wait for Docker Build**
```bash
# Monitor build on GitHub Actions tab
# After ~5-10 minutes, image available at:
# ghcr.io/YOUR-USERNAME/bemidb:latest
```

**4. Make Image Public** (Recommended)
```bash
# GitHub → Packages → bemidb → Settings → Change visibility → Public
```

**5. Create Deployment Repository**
```bash
cd ..
git clone https://github.com/YOUR-USERNAME/bemidb-deployment.git
cd bemidb-deployment

# Create structure (see DEPLOYMENT_PLAN_CLOUD_SCHEDULER.md)
mkdir -p config scripts docs

# Create config files with YOUR image URL
# (See full guide in DEPLOYMENT_PLAN_CLOUD_SCHEDULER.md)
```

**6. Deploy Infrastructure**
```bash
# Follow DEPLOYMENT_PLAN_CLOUD_SCHEDULER.md
./scripts/deploy-server.sh
./scripts/deploy-syncer.sh
./scripts/setup-cloud-scheduler.sh
```

---

### Ongoing Development

**When you need to update code:**

1. **Make changes in BemiDB fork**
   ```bash
   cd /path/to/BemiDB

   # Make your changes
   vim src/syncer-postgres/lib/syncer.go

   # Commit and push
   git add .
   git commit -m "Add new feature"
   git push origin main
   ```

2. **GitHub Actions auto-builds new image**
   - Triggered automatically on push to main
   - New image: `ghcr.io/YOUR-USERNAME/bemidb:latest`
   - Build time: ~5-10 minutes

3. **Redeploy to Fly.io** (uses new image)
   ```bash
   cd /path/to/bemidb-deployment

   ./scripts/deploy-server.sh    # Updates server
   ./scripts/deploy-syncer.sh    # Updates syncer

   # Cloud Scheduler automatically uses new image on next run
   ```

---

### Version Management

**Tagging Releases:**
```bash
cd /path/to/BemiDB

# Create version tag
git tag -a v1.7.0-custom.2 -m "Add feature X"
git push origin v1.7.0-custom.2

# GitHub Actions builds:
# - ghcr.io/YOUR-USERNAME/bemidb:latest
# - ghcr.io/YOUR-USERNAME/bemidb:v1.7.0-custom.2
# - ghcr.io/YOUR-USERNAME/bemidb:v1.7.0
```

**Using Specific Versions:**
```bash
# In deployment repo, update config/fly.toml:
[build]
  image = "ghcr.io/YOUR-USERNAME/bemidb:v1.7.0-custom.2"  # Pin to version

# Redeploy
./scripts/deploy-server.sh
```

---

## Repository Ownership

### Who Owns What?

**BemiDB Fork** (`YOUR-USERNAME/BemiDB`)
- **Owner**: You
- **Visibility**: Public (recommended) or Private
- **Purpose**: Customized BemiDB code
- **Updates**: Manually merge from upstream
- **Collaborators**: Your team

**BemiDB Deployment** (`YOUR-USERNAME/bemidb-deployment`)
- **Owner**: You
- **Visibility**: Private (recommended - contains infrastructure config)
- **Purpose**: Deployment automation
- **Updates**: As you modify infrastructure
- **Collaborators**: Your DevOps team

### Secrets Location

**GitHub Secrets** (BemiDB fork):
- `GITHUB_TOKEN` (automatic, for Docker push)

**Environment Variables** (Deployment repo):
- `.env` file (gitignored, never committed)
- Contains:
  - Database URLs
  - R2 credentials
  - Fly.io tokens
  - GCP project IDs

**Fly.io Secrets**:
- Set via `flyctl secrets set`
- Stored encrypted in Fly.io
- Used by both server and syncer

**Google Cloud Secrets**:
- Fly API token in Cloud Scheduler job headers
- Passed via `--headers` flag

---

## Troubleshooting

### Image Not Found

**Problem**: Fly.io can't pull `ghcr.io/YOUR-USERNAME/bemidb:latest`

**Solutions**:
1. Make package public (GitHub → Packages → bemidb → Settings)
2. Or configure Fly.io with registry auth:
   ```bash
   flyctl registry auth ghcr.io -u YOUR-USERNAME -p $GITHUB_PAT
   ```

### Build Failures

**Problem**: GitHub Actions build fails

**Debug**:
```bash
# Check Actions tab for errors
# Common issues:
# - Dockerfile syntax errors
# - Missing dependencies
# - Insufficient permissions

# Test build locally:
docker build -t test-bemidb .
```

### Deployment Conflicts

**Problem**: Multiple developers pushing to same Fly.io app

**Solution**:
```bash
# Use separate apps per developer/environment
# In deployment repo, create:
config/fly.dev.toml     # Development
config/fly.staging.toml # Staging
config/fly.prod.toml    # Production

# Deploy to specific environment:
flyctl deploy -c config/fly.dev.toml -a bemidb-server-dev
```

---

## Best Practices

### 1. Version Control

- ✅ **DO**: Tag releases in fork (`v1.7.0-custom.1`)
- ✅ **DO**: Commit infrastructure changes to deployment repo
- ❌ **DON'T**: Commit `.env` file (secrets)
- ❌ **DON'T**: Commit Fly.io state files

### 2. Security

- ✅ **DO**: Make Docker image public (no secrets in image)
- ✅ **DO**: Use Fly.io secrets for credentials
- ✅ **DO**: Rotate API tokens quarterly
- ❌ **DON'T**: Hardcode secrets in config files
- ❌ **DON'T**: Commit GCP credentials

### 3. Workflow

- ✅ **DO**: Test changes locally before pushing
- ✅ **DO**: Review GitHub Actions logs after build
- ✅ **DO**: Document custom changes in README
- ❌ **DON'T**: Deploy untested images to production
- ❌ **DON'T**: Skip version tags for releases

### 4. Maintenance

- ✅ **DO**: Sync fork with upstream periodically
- ✅ **DO**: Update dependencies when available
- ✅ **DO**: Monitor Fly.io and GCP costs
- ❌ **DON'T**: Let fork diverge too far from upstream
- ❌ **DON'T**: Ignore security updates

---

## Summary

**Two repositories working together:**

```
┌────────────────────────────────────┐
│   BemiDB Fork                      │
│   (Source code + modifications)    │
│                                     │
│   Push → GitHub Actions            │
│   Build → Docker Image             │
│   Publish → ghcr.io/YOU/bemidb     │
└────────────────┬───────────────────┘
                 │
                 ↓ Uses image
┌────────────────────────────────────┐
│   BemiDB Deployment                │
│   (Infrastructure config)          │
│                                     │
│   Scripts → Deploy to Fly.io       │
│   Scripts → Setup Cloud Scheduler  │
│   Config → fly.toml, .env          │
└────────────────────────────────────┘
```

**Result:**
- Clean separation between code and infrastructure
- Automated Docker builds on every push
- Reproducible deployments
- Easy version management
- Team collaboration friendly

---

For complete deployment instructions, see:
- **DEPLOYMENT_PLAN_CLOUD_SCHEDULER.md** - Full deployment guide
- **CLAUDE.md** - Development guide
