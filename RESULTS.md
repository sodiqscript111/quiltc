# Quiltc Production Verification (Backend API)

Date: 2026-03-01

This document verifies that `quiltc` (the CLI only) can operate Quilt’s Kubernetes-like control plane and container runtime by successfully calling the production Quilt backend HTTP API.

Secrets policy:
- No secrets are recorded in this doc (API keys, JWTs, node tokens).
- Local capture artifacts referenced below may contain secret-bearing JSON fields; redact before sharing.

## Environment

- Base URL: `https://backend.quilt.sh`
- Auth used for these runs: tenant API key via `X-Api-Key` (loaded from local `.env`, which is gitignored)

## Verified Capabilities

### 1. Container Runtime Session (Like “kubectl run/exec/logs/delete”)

Validated end-to-end with `X-Api-Key`:
- `GET /health`
- Containers:
  - `GET /api/containers`
  - `POST /api/containers`
  - `GET /api/containers/:id`
  - `GET /api/containers/:id/logs`
  - `POST /api/containers/:id/exec`
  - `GET /api/containers/:id/metrics`
  - `GET /api/containers/:id/network`
  - `POST /api/containers/:id/start`
  - `POST /api/containers/:id/stop`
  - `POST /api/containers/:id/kill`
  - `DELETE /api/containers/:id`
- Tenant-safe route programming inside the container netns:
  - `POST /api/containers/:id/routes`
  - `DELETE /api/containers/:id/routes`
- Events:
  - `GET /api/events` (SSE)

Evidence (local): `/tmp/quiltc_live_verify3`

### 2. API Key Management

Validated with `X-Api-Key`:
- `GET /api/api-keys`
- `POST /api/api-keys`
- `DELETE /api/api-keys/:id`

Evidence (local): `/tmp/quiltc_live_verify3` (contains secret-bearing JSON; do not share unredacted)

### 3. Volumes (Including File Push/Pull)

Validated with `X-Api-Key`:
- Volume lifecycle:
  - `GET /api/volumes`
  - `POST /api/volumes`
  - `GET /api/volumes/:name`
  - `DELETE /api/volumes/:name`
- Production file endpoints (JSON + base64):
  - Upload single file: `POST /api/volumes/:name/files`
  - Download single file: `GET /api/volumes/:name/files/*path` (CLI decodes base64 and writes bytes to disk)
- Production archive endpoint (implemented in CLI):
  - Upload/extract tar.gz: `POST /api/volumes/:name/archive`

Evidence (local): `/tmp/quiltc_live_verify6_path.txt` (points to the capture folder; includes SHA-256 match evidence)

### 4. Cluster Control Plane (Tenant Endpoints)

Validated with `X-Api-Key`:
- Clusters:
  - `POST /api/clusters`
  - `GET /api/clusters`
  - `GET /api/clusters/:cluster_id`
  - `POST /api/clusters/:cluster_id/reconcile`
  - `DELETE /api/clusters/:cluster_id`
- Workloads (CRUD):
  - `POST /api/clusters/:cluster_id/workloads`
  - `GET /api/clusters/:cluster_id/workloads`
  - `GET /api/clusters/:cluster_id/workloads/:workload_id`
  - `PUT /api/clusters/:cluster_id/workloads/:workload_id`
  - `DELETE /api/clusters/:cluster_id/workloads/:workload_id`
- Placements listing:
  - `GET /api/clusters/:cluster_id/placements`
- Nodes listing:
  - `GET /api/clusters/:cluster_id/nodes`

Evidence (local): `/tmp/quiltc_cluster_session1_path.txt`, `/tmp/quiltc_cluster_tenant_session_path.txt`

## Full “Kubernetes-Like” Cluster Management Session

In addition to exercising the tenant control-plane CRUD endpoints, a full session was run that mirrors the Kubernetes control loop:

- Create a cluster.
- Mint cluster-scoped join tokens, register nodes using join tokens, heartbeat nodes to `ready`.
- Create a workload with `replicas=3`.
- Reconcile to generate placements and assign replicas to nodes.
- Agent fetches placements for its node.
- Agent materializes placements by creating containers (tenant container runtime) and reports placement status (`running` + `container_id`).
- Delete a node and reconcile; observe placements rescheduled off the deleted node.
- Cleanup (delete workload, delete cluster, delete created containers).

Endpoints exercised in this session:

- Join tokens:
  - `POST /api/clusters/:cluster_id/join-tokens`

- Agent control-plane:
  - `POST /api/agent/clusters/:cluster_id/nodes/register`
  - `POST /api/agent/clusters/:cluster_id/nodes/:node_id/heartbeat`
  - `GET /api/agent/clusters/:cluster_id/nodes/:node_id/allocation`
  - `GET /api/agent/clusters/:cluster_id/nodes/:node_id/placements`
  - `POST /api/agent/clusters/:cluster_id/nodes/:node_id/placements/:placement_id/report`
  - `POST /api/agent/clusters/:cluster_id/nodes/:node_id/deregister`

- Tenant cluster control-plane:
  - `POST /api/clusters`
  - `GET /api/clusters`
  - `GET /api/clusters/:cluster_id`
  - `POST /api/clusters/:cluster_id/reconcile`
  - `GET /api/clusters/:cluster_id/nodes`
  - `GET /api/clusters/:cluster_id/nodes/:node_id`
  - `POST /api/clusters/:cluster_id/nodes/:node_id/drain`
  - `DELETE /api/clusters/:cluster_id/nodes/:node_id`
  - `POST /api/clusters/:cluster_id/workloads`
  - `GET /api/clusters/:cluster_id/workloads`
  - `GET /api/clusters/:cluster_id/workloads/:workload_id`
  - `PUT /api/clusters/:cluster_id/workloads/:workload_id`
  - `DELETE /api/clusters/:cluster_id/workloads/:workload_id`
  - `GET /api/clusters/:cluster_id/placements`
  - `DELETE /api/clusters/:cluster_id`

Evidence (local):
- Full kube-like session with join tokens + 2 nodes, 3 replicas, placement reporting, and reschedule: `/tmp/quiltc_kube_session_join_path.txt` (points to capture folder).
- Node endpoints + allocation + agent placement assignment verification: `/tmp/quiltc_cluster_endpoints_verify2_path.txt` (points to capture folder).

## Kubernetes Manifest Command Surface (`quiltc k8s`)

Status (2026-03-01):
- Backend workflows completed and integrated with CLI.
- Implemented in CLI with command and behavior coverage.
- Production-ready backend-driven `/api/k8s/*` workflows.
- Bi-directional Kubernetes backend compatibility is part of the delivered workflow surface.

Implemented commands:
- `quiltc k8s validate -f <file|dir|url>`
- `quiltc k8s apply -f <file|dir|url> --cluster-id <id>`
- `quiltc k8s diff -f <file|dir|url> --cluster-id <id>`
- `quiltc k8s status --operation <id> --cluster-id <id>`
- `quiltc k8s get resources --cluster-id <id> [filters]`
- `quiltc k8s get resource <resource_id> --cluster-id <id>`
- `quiltc k8s delete <resource_id> --cluster-id <id>`
- `quiltc k8s export --cluster-id <id> -o yaml|json`
- `quiltc k8s schema`

CLI behavior coverage:
- Deterministic local manifest collection from file/dir/url.
- Raw manifest payload pass-through to backend `/api/k8s/*` using `manifest` string field.
- Validate-before-apply default (`--no-validate` support).
- `--dry-run` routes through validate/diff (no apply mutation).
- `--strict` warning-to-failure behavior.
- Stable exit-code mapping for CI (`0/2/3/4`).

Evidence (local):
- CLI test suite: `cli/tests/k8s_cli.rs`
- Help surface: `cargo run -p quiltc -- k8s --help`
