# Local Testing for ClickHouse Headless Service

This guide explains how to test the ClickHouse headless service feature locally, enabling DNS resolution of Kubernetes headless services from your development machine.

## Prerequisites

- `kubectl` configured with access to your Kubernetes cluster
- `dig` for testing DNS resolution (usually pre-installed)

## Overview

When `use_headless_service = true` is enabled, Vector resolves the endpoint hostname via DNS to discover individual ClickHouse pod IPs. In Kubernetes, this works automatically. Locally, we need to:

1. Port-forward kube-dns from the cluster
2. Configure Vector to use the custom DNS server via the `dns_server` config option

```
Vector (hickory-resolver)
  → 127.0.0.1:5353 (kubectl port-forward, TCP)
  → kube-dns in cluster
  → Returns pod IPs
```

**Note:** The `dns_server` option bypasses the system resolver entirely, making local testing much simpler.

## Step 1: Port-Forward kube-dns

Open a terminal and run:

```bash
kubectl port-forward -n kube-system svc/kube-dns 5353:53 --address 0.0.0.0
```

Verify it works:

```bash
dig @127.0.0.1 -p 5353 +tcp <your-headless-service>.<namespace>.svc.cluster.local
```

Example:
```bash
dig @127.0.0.1 -p 5353 +tcp cluster-service-write-headless-testing.logging-clickhouse.svc.cluster.local
```

You should see multiple A records (one per pod).

## Step 2: Configure Vector

Create or update your local config file (e.g., `local-clickhouse.toml`):

```toml
[sources.demo]
type = "demo_logs"
format = "json"
interval = 1.0

[sinks.clickhouse]
type = "clickhouse"
inputs = ["demo"]
endpoint = "http://cluster-service-write-headless-testing.logging-clickhouse.svc.cluster.local:8123"
table = "vector_logs"
database = "default"
format = "json_each_row"
date_time_best_effort = true
skip_unknown_fields = true

# Enable headless service mode
use_headless_service = true
dns_refresh_interval_secs = 30

# Custom DNS server for local testing (point to kubectl port-forward)
# This bypasses the system resolver and queries kube-dns directly via TCP
dns_server = "127.0.0.1:5353"

[sinks.clickhouse.batch]
max_bytes = 1048576
timeout_secs = 1
```

**Note:** The `dns_server` option tells Vector to use a custom DNS server instead of the system resolver. This is the key setting for local testing - it queries the port-forwarded kube-dns directly via TCP, avoiding all the system DNS configuration complexity.

## Step 3: Run Vector

```bash
cargo run -- --config ./local-clickhouse.toml
```

You should see logs indicating successful DNS resolution:

```
INFO vector::sinks::clickhouse::headless: HeadlessService initialized for ClickHouse.
     endpoint=http://cluster-service-write-headless-testing.logging-clickhouse.svc.cluster.local:8123/
     active_endpoints=3
     ips=["10.x.x.1", "10.x.x.2", "10.x.x.3"]
```

## Cleanup

When done testing:

```bash
# Stop kubectl port-forward (Ctrl+C in that terminal)
```

## Troubleshooting

### DNS resolution still failing

1. Check that kubectl port-forward is still running
2. Verify the port-forward works directly:
   ```bash
   dig @127.0.0.1 -p 5353 +tcp cluster-service-write-headless-testing.logging-clickhouse.svc.cluster.local
   ```
3. Ensure `dns_server = "127.0.0.1:5353"` is set in your config

### Connection refused or timeout

The port-forward may have died. Restart it:
```bash
kubectl port-forward -n kube-system svc/kube-dns 5353:53 --address 0.0.0.0
```

### Wrong cluster context

Verify you're connected to the right cluster:
```bash
kubectl config current-context
kubectl get pods -n logging-clickhouse
```

## Alternative: Use Telepresence

If you can install Telepresence's traffic-manager in your cluster, it handles all DNS routing automatically:

```bash
# One-time cluster setup (requires helm)
telepresence helm install

# Connect from your machine
telepresence connect

# Now all .cluster.local names resolve automatically
cargo run -- --config ./local-clickhouse.toml

# Disconnect when done
telepresence quit
```
