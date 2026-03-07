# Helm Deployment Layout

This repository keeps Docker Compose assets for local/legacy use and adds Helm assets for EKS.

## Releases

- `traefik` in namespace `traefik` using `helm/values/traefik.yaml`
- `cnpg-operator` in namespace `cnpg-system` using `helm/values/cnpg-operator.yaml`
- `shuttle-cnpg` in namespace `data` using `charts/shuttle-cnpg`
- `prometheus` in namespace `monitoring` using `helm/values/prometheus.yaml`
- `grafana` in namespace `monitoring` using `helm/values/grafana.yaml`
- `shuttle-monitoring-resources` in namespace `monitoring` using `charts/shuttle-monitoring-resources`
- `otp` in namespace `app` using `charts/otp`
- `shuttle-backend` in namespace `app` using `charts/shuttle-backend`

Custom charts have matching override files:

- `helm/values/shuttle-cnpg.yaml`
- `helm/values/shuttle-monitoring-resources.yaml`
- `helm/values/otp.yaml`
- `helm/values/shuttle-backend.yaml`

## Environment Overrides

- `dev` overrides: `data/helm/values/dev`
- `prd` overrides: `data/helm/values/prd`

Context mapping:

- `dev`: `arn:aws:eks:ap-northeast-1:499666785507:cluster/dev-bk-bot`
- `prd`: `arn:aws:eks:ap-northeast-1:499666785507:cluster/bk-bot`

Use base + env values together:

```bash
ENV=dev # dev | prd
if [ "${ENV}" = "dev" ]; then
  CTX=arn:aws:eks:ap-northeast-1:499666785507:cluster/dev-bk-bot
else
  CTX=arn:aws:eks:ap-northeast-1:499666785507:cluster/bk-bot
fi

helm upgrade --install traefik traefik/traefik \
  --namespace traefik \
  --create-namespace \
  --kube-context "${CTX}" \
  -f helm/values/traefik.yaml \
  -f data/helm/values/${ENV}/traefik.yaml
```

## Suggested Install Order

1. Install `traefik`
2. Install `cnpg-operator`
3. Install `shuttle-cnpg`
4. Install `prometheus`
5. Install `grafana`
6. Install `shuttle-monitoring-resources`
7. Install `otp`
8. Install `shuttle-backend`

## Notes

- Replace placeholder hosts, volume IDs, secrets, and storage sizes before applying.
- Keep public-safe defaults in `helm/values`, and put real/private overrides in `data/helm/values/<env>` (gitignored).
- External exposure is Cloudflare Tunnel based. `cloudflared` should forward traffic to `http://traefik.traefik.svc.cluster.local:80`.
- Traefik service is `ClusterIP`, and HTTP routing is handled by host rules:
  - `api.example.com` -> backend
  - `grafana.example.com` -> grafana
- cert-manager is not used in this layout.
- `shuttle-backend` assumes `data.db` is manually copied into the mounted EBS volume.
- `shuttle-backend` disables in-app background workers and moves cleanup work to CronJobs.
- `backend-env` should provide sensitive backend env vars, including `USER_STORE_POSTGRES_PASSWORD`.
- `grafana-env` should provide `USER_STORE_POSTGRES_HOST`, `USER_STORE_POSTGRES_PORT`, `USER_STORE_POSTGRES_DB`, `USER_STORE_POSTGRES_USER`, and `USER_STORE_POSTGRES_PASSWORD` for datasource interpolation.
- `db-app` and `db-superuser` secrets must exist in `data` namespace before `shuttle-cnpg` install.
- CNPG runs as `Primary 1 + Replica 2` with `gp2` storage (`200Gi`) and `rw` pooler (`2` replicas).
- CNPG backup targets S3 (`s3://<cluster.backup.bucketName>/shuttle-go/prod/cnpg`) with WAL archiving and daily scheduled backups. Set `cluster.backup.bucketName` and `cluster.irsaRoleArn` in `data/helm/values/<env>/shuttle-cnpg.yaml` when IAM role is ready.
- Prometheus now scrapes `cloudflared` metrics via `cloudflared/cloudflared-metrics` service endpoints and CNPG pod metrics directly via pod discovery. `cloudflared` must expose metrics on its metrics service.
- Prometheus additionally scrapes `pgbouncer` (`db-rw-pooler-*` pods on `:9127`) and `cnpg-operator` (`cnpg-system` operator pods on `:8080`).
- Grafana dashboard ConfigMaps include API, Search BI, Infra Runtime, CNPG Runtime, K8s Resources Overview, K8s Workload Health, PVC/EBS Capacity, Traefik Runtime, Cloudflared Runtime, PgBouncer Runtime, Prometheus Self Monitoring, and CNPG Backup/Replication dashboards.
