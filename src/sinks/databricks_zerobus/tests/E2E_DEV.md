# E2E auth verification on dev

How to validate the `databricks_zerobus` sink with `login_service` auth from a
dev nimbus pod end-to-end. This proves the full chain:

```
vector (in pod) → mTLS → pop-proxy → Login → JWT
                                              │
                  ── Bearer JWT ──→ RIG / storage-proxy → Shinkansen → Zerobus
```

## Prerequisites

- A vector checkout at `~/vector` with the branch under test.
- A vector release binary built with the right features:
  ```bash
  cd ~/vector
  cargo build --release --bin vector --features sinks-databricks-zerobus,codecs-arrow
  strip -o /tmp/vector-stripped target/release/vector
  ```
- A dev nimbus pod with the dbts2 UWI cert. Any `Running` `bricklens-agent-nimbus-*`
  pod on a `dev-aws-uw2-s1-*` cluster works. Extract its workspace ID from the
  cert SAN OID `1.3.6.1.4.1.42.113.1`, or relabel its node with the workspace
  you want (see "Targeting a specific workspace" below).
- An OAuth-service protobuf descriptor at `/tmp/oauth_service.pb`, generated
  via the universe `proto_descriptor_set` rule against
  `//common/authentication/oauth/proto:oauth_proto_library` and copied into
  the pod.

## Streaming files into the pod

`bin/dbctl exec` does NOT propagate stdin EOF, so naive `... | dbctl exec --
  base64 -d > /tmp/foo` leaves the pod-side decoder waiting indefinitely.
Use a **sentinel marker + `awk`** pattern, and gzip large payloads first.

Save this as `/tmp/stream_to_pod.sh` (works for any gzipped file):

```bash
#!/bin/bash
set -uo pipefail

POD="${POD:?POD required}"
CTX="${CTX:?CTX required}"
NS="${NS:-logging-agent}"
CTR="${CTR:-vector-daemon}"
SRC_GZ="${SRC_GZ:?SRC_GZ required}"
DST="${DST:?DST required}"

REMOTE_CMD='awk "/^__END_OF_PAYLOAD__$/ {exit} {print}" | base64 -d | gunzip > '"$DST"' && chmod +x '"$DST"' && echo "COPY_DONE size=$(stat -c %s '"$DST"')"'

{
  base64 < "$SRC_GZ"
  echo "__END_OF_PAYLOAD__"
} | bin/dbctl exec -t "$CTX" -n "$NS" "$POD" -c "$CTR" --dbcert -- sh -c "$REMOTE_CMD"
```

Then gzip each file (the 244 MB binary compresses to ~92 MB and lands in ~3 min):

```bash
chmod +x /tmp/stream_to_pod.sh

# Vector binary
gzip -1 -k /tmp/vector-stripped
POD=bricklens-agent-nimbus-XXXXX CTX=dev-aws-uw2-s1-0-XXXXXX \
  SRC_GZ=/tmp/vector-stripped.gz DST=/tmp/vector /tmp/stream_to_pod.sh
# Expect: COPY_DONE size=<~255_000_000>

# OAuth proto descriptor (built with bazel proto_descriptor_set; ~414 KB)
gzip -1 -k /tmp/oauth_service.pb
POD=... CTX=... SRC_GZ=/tmp/oauth_service.pb.gz DST=/tmp/oauth_service.pb \
  /tmp/stream_to_pod.sh

# Vector config
gzip -1 -k /tmp/vector.yaml
POD=... CTX=... SRC_GZ=/tmp/vector.yaml.gz DST=/tmp/vector.yaml \
  /tmp/stream_to_pod.sh

# Table schema descriptor (matching the target UC table exactly)
gzip -1 -k /tmp/test_otel_logs.desc
POD=... CTX=... SRC_GZ=/tmp/test_otel_logs.desc.gz DST=/tmp/test_otel_logs.desc \
  /tmp/stream_to_pod.sh
```

If COPY_DONE doesn't appear, kill orphan decoder processes (they hold `/tmp/<dst>`
open and block re-streams):

```bash
bin/dbctl exec -t $CTX -n logging-agent $POD -c vector-daemon --dbcert -- sh -c '
  for pid in $(ls -la /proc/*/fd/* 2>/dev/null | grep "/tmp/vector" |
               grep -oE "/proc/[0-9]+" | cut -d/ -f3 | sort -u); do
    kill -9 $pid 2>/dev/null && echo killed $pid
  done
  rm -f /tmp/vector
'
```

## Targeting a specific workspace

The dev `bricklens-agent-nimbus` daemonset's UWI cert is workspace-bound: the
cert SAN OID `1.3.6.1.4.1.42.113.1` encodes the workspace ID from the node's
`orgId` label. Login validates that `Authentication.workspace` in the request
matches that workspace.

**To re-target a Running pod to a different workspace WITHOUT a pod restart:**

1. Find the pod's node:
   ```bash
   bin/dbctl get pod -t $CTX -n logging-agent $POD -o json |
     jq -r '.spec.nodeName'
   ```
2. Relabel the node with the target workspace's `orgId`:
   ```bash
   kubectl --context $CTX label node <node-name> \
     orgId=<target_workspace> --overwrite
   ```
3. Poll the in-pod cert until the SAN OID hex tail matches the ASCII hex of
   `<target_workspace>` (typically 15–45 s):
   ```bash
   target_hex=$(printf '%s' '<target_workspace>' | xxd -p)
   until bin/dbctl exec -t $CTX -n logging-agent $POD -c vector-daemon \
     --dbcert -- openssl asn1parse -in /databricks-infra/dbts2/creds.combined.pem \
     -inform pem 2>/dev/null | grep -A1 '1.3.6.1.4.1.42.113.1' |
     tr -d '[:space:]' | grep -q "$target_hex"; do sleep 5; done
   ```

The node-side dblet service rewrites the bytes at the `dbts2` hostPath
(`/var/dblet/config_data/secret/dbts2/apps/bricklens-agent/creds.combined.pem`)
on label change; the pod sees the new bytes through its existing mount.

This works because every dev `bricklens-agent-nimbus` pod that's *not* old enough
to predate the current daemonset configmap crashloops on a pre-existing VRL
config error in `serverless_gpu_compute_transformed` — so deleting the pod to
force a restart with the new workspace doesn't work; relabel-without-restart is
required.

**Tested workspace** (works with the current dev SAFE-flag setup):
- `1426760070658584` — `feature-store-us-west-2`, `oregon-dev` shard.
  Workspace URL: `https://feature-store-us-west-2.dev.databricks.com/`.

Always restore the original `orgId` label after testing so you don't strand
another team's microVM assignment.

## Vector config

A working `/tmp/vector.yaml`:

```yaml
data_dir: /tmp/vector-data

sources:
  one_shot:
    type: demo_logs
    format: shuffle
    lines:
      - "test event"
    sequence: true
    interval_secs: 5

transforms:
  shape:
    type: remap
    inputs: [one_shot]
    source: |
      .timestamp = to_unix_timestamp(now(), unit: "nanoseconds")
      .body = "test event"
      del(.message)
      del(.timestamp_str)
      del(.source_type)
      del(.service)
      del(.host)

sinks:
  zerobus:
    type: databricks_zerobus
    inputs: [shape]
    table_name: shinkansen.default.test_otel_logs
    ingestion_endpoint: "https://<workspace_id>.zerobus.us-west-2.dev.databricks.com"
    unity_catalog_endpoint: "https://unused.dev.databricks.com"
    schema:
      type: path
      path: /tmp/test_otel_logs.desc
      message_type: otel.OtelLog
    batch_encoding:
      codec: proto_batch
    auth:
      strategy: login_service
      workspace_id: <workspace_id>
      service_principal_resource: "accounts/0829fba4-55f5-4fef-9850-00f15b1a3d73/apps"
      login_endpoint: "https://login-service-bricklens.privileged.dev.dbns.databricks.com:443"
      login_server_name: "login-service-bricklens.privileged.dev.dbns.databricks.com"
      tls_crt_file: "/databricks-infra/dbts2/creds.combined.pem"
      tls_key_file: "/databricks-infra/dbts2/creds.combined.pem"
      tls_ca_file: "/databricks-infra/dbts2/creds.combined.pem"
      oauth_proto_descriptor_path: "/tmp/oauth_service.pb"
      uc_permissions:
        - privileges: [USE_CATALOG]
          securable_type: CATALOG
          full_name: shinkansen
        - privileges: [USE_SCHEMA]
          securable_type: SCHEMA
          full_name: shinkansen.default
        - privileges: [SELECT, MODIFY]
          securable_type: TABLE
          full_name: shinkansen.default.test_otel_logs
```

**Config gotchas:**
- `data_dir` must be a writable path; the container's default `/var/lib/vector/`
  doesn't exist on the bricklens-agent image.
- `tls_ca_file` MUST be set. Vector's `openssl` rejects the Login server cert
  with `tlsv1 alert unknown ca` because the container's `/etc/ssl/certs` lacks
  the Databricks "Data Plane Misc Root". The `creds.combined.pem` already
  bundles the root chain, so pointing `tls_ca_file` at the same file is fine.
- The proto schema must match the UC table column-for-column.
  `shinkansen.default.test_otel_logs` is just `{int64 timestamp, string body}`;
  using a wider OtelLog fixture fails with Zerobus
  `Error Code: 4027 Field "x" found in the proto definition, but not in the table schema`.

## Run vector inside the pod

```bash
bin/dbctl exec -t $CTX -n logging-agent $POD -c vector-daemon --dbcert -- \
  /tmp/vector validate /tmp/vector.yaml
```

A passing healthcheck (which exercises OAuth bootstrap + Zerobus stream
creation) prints `√ Validated`.

To actually emit a record (instead of just running healthchecks), run vector
with a timeout:

```bash
bin/dbctl exec -t $CTX -n logging-agent $POD -c vector-daemon --dbcert -- \
  timeout 15 /tmp/vector --config /tmp/vector.yaml
```

## Pass criteria

In vector's stderr, look for:

1. `Bootstrapping OAuth token from Login service.` (info)
2. `OAuth token bootstrap successful.` with `expires_in_secs` near 3600 (info)
3. `Successfully created stream stream_id=<uuid>` (Zerobus accepted the proto schema)
4. `Healthcheck passed.` and `√ Validated`
5. On shutdown: `Stream is caught up to offset N. Waiting for offset N+1.` then
   `Stream is caught up to the given offset. Flush completed.` — confirms N
   events landed.

What you should NOT see:

- `Failed to get OAuth token from Login service` → Login mint failed; check
  `service_principal_resource`, `workspace_id` (must match the cert's UWI
  workspace), and the cert path.
- `IllegalStateException: authorizationDetailsClaim must be defined` (visible
  only in storage-proxy JVM sidecar logs, not in vector's output) → JWT didn't
  carry the claim; this would be a regression of the PR #1855740 fix.
- `Invalid token audience` → the audience SAFE flag isn't enabled for this
  workspace, or `workspace_id` doesn't match the cert.
- `tlsv1 alert unknown ca` → `tls_ca_file` not set (see Config gotchas).
- `Field "x" found in the proto definition, but not in the table schema` →
  the descriptor at `schema.path` doesn't match the UC table.

## Cross-checking the JWT

For direct JWT inspection (decoded claims, `authorization_details` payload),
use the curl harness in
`bricklens/.claude/skills/bricklens-zerobus-auth/SKILL.md`.

## Cleanup

```bash
bin/dbctl exec -t $CTX -n logging-agent $POD -c vector-daemon --dbcert -- \
  rm -f /tmp/vector /tmp/vector.yaml /tmp/oauth_service.pb /tmp/test_otel_logs.desc
```

Also restore the node's original `orgId` label if you relabelled it.

## What this does NOT cover

- Records actually landing in the table queryable through SQL warehouse —
  the schema validation pass above only confirms Zerobus accepted the proto
  descriptor; for end-user query verification, query the UC table directly
  after the run.
- Token re-bootstrap (JWT TTL 3600 s; the proactive loop fires at expiry−300 s).
  To test, run vector for >55 min and watch for `Proactive token re-bootstrap successful.`.
