# quiltc

`quiltc` is Quilt's Kubernetes-like cluster-management layer.

Repo roles:
- `cli/`: user-facing CLI over Quilt HTTP APIs
- `control/`: higher-level orchestrator/controller
- `agent/`: node-local mesh agent

Authority boundary:
- `quilt-prod` is the only source of truth for clusters, nodes, placements, allocations, and runtime/container state
- `quiltc/control` consumes that HTTP control plane and drives orchestration workflows
- `quiltc/agent` consumes backend node/peer assignments and realizes mesh state locally against the Quilt daemon

## Build

```bash
cargo build -p quiltc
cargo build -p quilt-mesh-control
cargo build -p quilt-mesh-agent
```

Binary path on macOS (Apple Silicon): `target/aarch64-apple-darwin/debug/quiltc`

## Configuration

`quiltc` reads auth and base URL from flags and/or environment variables:

- `QUILT_BASE_URL` (e.g. `https://backend.quilt.sh`)
- `QUILT_API_KEY` (tenant key, sent as `X-Api-Key`)
- `QUILT_JWT` (tenant JWT, sent as `Authorization: Bearer ...`)

Optional:
- `--save-auth` stores the provided tenant auth in `~/.config/quiltc/config.json` (permissions best-effort `0600` on Unix).

Agent enrollment uses join tokens:
- `QUILT_JOIN_TOKEN` (cluster-scoped, short-lived join token, sent as `X-Quilt-Join-Token` on node registration)

## Quick Start (Cluster + Nodes + Workload)

Create a cluster:

```bash
quiltc clusters create \\
  --name demo \\
  --pod-cidr 10.70.0.0/16 \\
  --node-cidr-prefix 24
```

Mint a join token:

```bash
quiltc clusters join-token-create <cluster_id> --ttl-secs 600 --max-uses 1
```

Register a node (repeat per node; join tokens are typically single-use):

```bash
quiltc agent register <cluster_id> --join-token <join_token> \\
  --name node-a \\
  --public-ip 203.0.113.10 \\
  --private-ip 10.0.0.10 \\
  --agent-version quiltc-test \\
  --labels-json '{}' \\
  --taints-json '{}' \\
  --bridge-name quilt0 \\
  --dns-port 53 \\
  --egress-limit-mbit 0
```

Heartbeat node to `ready`:

```bash
quiltc agent heartbeat <cluster_id> <node_id> --state ready
```

Create a workload (replicated desired-state):

```bash
quiltc clusters workload-create <cluster_id> \\
  '{\"name\":\"demo\",\"replicas\":3,\"command\":[\"sh\",\"-lc\",\"echo hi; tail -f /dev/null\"],\"memory_limit_mb\":128}'
```

Reconcile + observe placements:

```bash
quiltc clusters reconcile <cluster_id>
quiltc clusters placements <cluster_id>
```

## Runtime Operations (Containers)

Create a container:

```bash
quiltc containers create '{\"name\":\"demo\",\"command\":[\"sh\",\"-lc\",\"echo hi; tail -f /dev/null\"],\"memory_limit_mb\":128}'
quiltc containers create '{\"name\":\"demo\",\"image\":\"alpine:3.20\"}' --async-mode false
```

Batch create containers:

```bash
quiltc containers batch-create '[{\"name\":\"a\",\"image\":\"alpine:3.20\"},{\"name\":\"b\",\"image\":\"alpine:3.20\"}]'
```

Exec:

```bash
quiltc containers exec <container_id> -- sh -lc 'id && ip addr && ip route'
```

Logs:

```bash
quiltc containers logs <container_id>
```

Snapshot/fork/resume lifecycle:

```bash
quiltc containers snapshot <container_id> --wait
quiltc containers fork <container_id> --wait
quiltc containers resume <container_or_snapshot_id> --wait
quiltc containers stop <container_id> --async-mode true
quiltc containers delete <container_id> --async-mode false
quiltc snapshots clone <snapshot_id> --async-mode true
```

Snapshot management:

```bash
quiltc snapshots list --container-id <container_id>
quiltc snapshots get <snapshot_id>
quiltc snapshots lineage <snapshot_id>
quiltc snapshots clone <snapshot_id> --wait
quiltc snapshots pin <snapshot_id>
quiltc snapshots unpin <snapshot_id>
quiltc snapshots delete <snapshot_id>
```

Operation status:

```bash
quiltc operations get <operation_id>
quiltc operations watch <operation_id> --timeout-secs 300
```

Events (SSE):

```bash
quiltc events
quiltc events --operation-id <operation_id>
```

## Volumes (File Push/Pull)

Upload a file into a volume (JSON + base64 behind the scenes):

```bash
quiltc volumes upload <volume_name> ./local.txt --path /remote.txt
```

Download a file from a volume:

```bash
quiltc volumes download <volume_name> ./out.txt --path /remote.txt
```

Upload/extract a `.tar.gz` archive:

```bash
quiltc volumes archive-upload <volume_name> ./bundle.tar.gz --path / --strip-components 0
```

## Escape Hatch

For new/experimental endpoints not yet wrapped by a subcommand:

```bash
quiltc request GET /api/clusters
quiltc request POST /api/clusters/<cluster_id>/join-tokens --json '{\"ttl_secs\":600,\"max_uses\":1}'
```

## Kubernetes Manifests (Backend-Driven)

`quiltc` keeps the Kubernetes-like operator UX while supporting bi-directional Kubernetes backend compatibility. The CLI does local manifest collection only (file/dir/url), then sends raw payloads to backend `/api/k8s/*` endpoints. The CLI does not perform Kubernetes translation logic.

```bash
quiltc k8s validate -f ./manifests --namespace default
quiltc k8s apply -f ./manifests --cluster-id <cluster_id> --application default --follow
quiltc k8s apply -f ./manifests --cluster-id <cluster_id> --dry-run
quiltc k8s diff -f ./manifests --cluster-id <cluster_id>
quiltc k8s status --operation <operation_id> --cluster-id <cluster_id> --follow
quiltc k8s get resources --cluster-id <cluster_id> --kind Deployment
quiltc k8s get resource <resource_id> --cluster-id <cluster_id>
quiltc k8s delete <resource_id> --cluster-id <cluster_id>
quiltc k8s export --cluster-id <cluster_id> -o yaml
quiltc k8s schema
```

Behavior:
- Request contract uses single `manifest` YAML string (multi-doc supported with `---`).
- `apply` and `diff` require `--cluster-id`; `status` and resource paths are cluster-scoped.
- `apply` validates first by default (use `--no-validate` to skip).
- `--dry-run` uses validate+diff flow without apply.
- `--strict` treats backend warnings as failures.
- `--json` prints backend JSON as-is for machine use.
- Manifest order is deterministic across directory inputs (`.yaml`, `.yml`, `.json`, lexical order).

K8s CI exit codes:
- `0`: success
- `2`: validation failure
- `3`: apply/delete/operation failure
- `4`: transport or auth failure

## Docs

- `RESULTS.md` for live verification evidence and the exact endpoint coverage.
- `K8S.md` for the Kubernetes parity mapping.
