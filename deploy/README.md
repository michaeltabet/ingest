# deploy — k8s, gitops only

Every cluster change is a gitops MR reconciled by ArgoCD. NOTHING here is
applied by hand. Internal GUIs (Django admin, Airflow) are tailnet-only behind
the standard SSO path. Images built on the Mac (pinned tags, tailnet registry —
CI has no docker runner). The architecture map is updated in the same MR.

Runnable units (each its own image + Deployment):

| Unit | Image | Scales | Notes |
|---|---|---|---|
| `worker-http` | ingest + engine | WIDE (many small pods) | Temporal worker, queue `scrape-http` |
| `worker-browser` | ingest + **browser baked in** | NARROW (few fat pods) | queue `scrape-browser` |
| `airflow` | official chart + ingest | — | KubernetesExecutor, CNPG metadata PG |
| `debezium` | Kafka Connect | — | registers `streaming/debezium/*` |
| `control` | ingest (Django) | 1 | admin, tailnet-only |
| `ch-migrate` | ingest (job) | one-shot | applies `clickhouse/migrations/*` |

Slot counts / replicas / memory come from calibrated hypotheses, seeded from
stated guesses. OOM-kills are data, not incidents (fail-loud doctrine).

STATUS: manifests added in build-order step 7, step by step, with explicit go.
Nothing deploys before Michael says yes.
