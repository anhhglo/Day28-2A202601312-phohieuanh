# Bằng chứng chạy thật — Day 28 Track 2

Chạy trên WSL2 (Ubuntu, 20 CPU, 23,5 GB RAM), `docker compose --profile full`,
14 service `running/healthy`. Mọi ID dưới đây lấy từ `evidence/*.json` do
`uv run lab28 evidence` và bộ integration test sinh ra, không chép tay.

## Điểm số tổng hợp

`evidence/integration-report.json` — **score 83/100**, 6 điểm verified,
5 điểm passing, 4 điểm unverified-from-process (được chứng minh bằng file
evidence riêng), 1 điểm `not_ready` là IP07 vì máy không có GPU.

## 1. Luồng chạy đúng (J1 golden path)

| Bước | Bằng chứng | Giá trị |
|---|---|---|
| IP08 client → Envoy → API | `ip08-gateway.json` | `x-request-id=820be555-e514-9898-8a90-e186bea5080b`, 200 |
| IP01 API → Kafka | `ip01-kafka-consume.json` | topic `data.raw` partition 2 offset 40, `idempotency-key=it-j1-dac45467`, header `traceparent` có mặt |
| IP02 Kafka → Airflow | `ip02-airflow-run.json` | dag_run `it-fb38d064` = `success`; 4 asset event: `lab28://delta/{documents,feedback}`, `lab28://qdrant/lab28_documents`, `lab28://feast/asker_activity` |
| IP03 Airflow → Delta | `ip03-delta-history.json` | `feedback` version 25, `documents` version 16, toàn bộ operation là `MERGE` |
| IP04 Delta → Feast | `ip04-feast-online.json` | entity `asker_id=it-j1-dac45467`, `delta_version=20`, mọi feature `PRESENT` |
| IP05 Delta → Qdrant | `ip05-qdrant-search.json` | 24 point, hybrid search trả `doc-ip09-metrics` score 0,833 |
| IP06 Delta → MLflow | `ip06-mlflow-release.json` | `lab28-rag-release v3 is champion` |
| IP10 trace xuyên luồng | `ip10-trace.json` | trace `52fccfa7a317447989d7cb6bd6909c16`, 11 span, 3 service (`lab28-gateway`, `lab28-api`, `lab28-airflow`) |

Chuỗi span đã có trong một trace: `lab28.gateway.request` → `lab28.api.ingest`
→ `lab28.kafka.produce` → `lab28.kafka.consume` → `lab28.airflow.dag` →
`lab28.spark.delta_merge`. Các span `lab28.api.ask`, `lab28.qdrant.query`,
`lab28.vllm.chat_completion` **thiếu** vì đường `/ask` dừng ở vLLM — xem mục IP07.

## 2. Replay-safe (J2)

`uv run pytest integration-tests/test_j2_idempotent_replay.py -q` → **9 passed**.
Nội dung được chứng minh: gửi lại cùng một lô bản tin thì Kafka giữ đủ mọi lần
giao (mỗi lần là một `event_id` riêng), nhưng Delta chỉ có **một** hàng cho mỗi
`idempotency_key`, Feast đếm sự kiện **một** lần, Qdrant giữ **đúng một** point
cho mỗi tài liệu. Delta version *được phép* tăng dù số hàng không đổi — đó là
đặc tính của MERGE, và test khẳng định đúng điều đó.

Cơ chế: `dedupe_latest` gộp lô theo `idempotency_key` trước khi vào MERGE (Delta
sẽ lỗi nếu source có hai hàng cùng khớp một hàng target), chọn bản có
`(occurred_at, event_id)` lớn nhất nên kết quả không phụ thuộc thứ tự Kafka giao.

## 3. Metrics (IP09)

`ip09-prometheus-targets.json` — 9/10 target `up`:
`lab28-api`, `lab28-gateway`, `lab28-kafka`, `lab28-qdrant`, `lab28-feast`,
`lab28-mlflow`, `lab28-collector`, `lab28-prometheus`, `lab28-airflow-batch`.
Target duy nhất `down` là `lab28-vllm-optional` (không có GPU).
Alert rule đã nạp, ví dụ `Lab28ApiUnavailable` (severity `critical`, for 30s).

`ip09-grafana-dashboards.json` — dashboard `Lab 28 Platform Overview`
(uid `lab28-platform`), datasource Prometheus.

Load profile (`load-profile.txt`): qua gateway 200 request/8 worker → 13×200 +
187×429, chứng minh rate limit 10 rps của Envoy chặn đúng; đo trực tiếp FastAPI
`/ready` → 200/200 thành công, p50 314 ms, p95 621 ms, p99 852 ms.

## 4. Kubernetes / GitOps (IP08 tầng triển khai)

`gitops-drill.log` — máy chạy bài không có cluster nên phần này chứng minh ở
tầng **desired state**, thứ Argo CD thực sự đối chiếu, và nói rõ là không giả
lập output của cluster:

- `scripts/validate_manifests.py` PASS: đủ 9 kind bắt buộc (Deployment, Service,
  ServiceAccount, ConfigMap, HPA, PodDisruptionBudget, NetworkPolicy, Gateway,
  HTTPRoute), Deployment chạy non-root.
- `gitops/application.yaml`: `targetRevision: refs/tags/v3.0.0` (tag bất biến →
  rollback là trỏ về tag trước, không sửa mã), `selfHeal: true` (hoàn tác thay
  đổi thủ công), `prune: true` (xoá khỏi git thì xoá khỏi cluster),
  `revisionHistoryLimit: 5`.
- Drift injected: `replicas: 2 → 9`. Điểm đáng nói là validator **vẫn PASS** —
  manifest lệch nhưng không sai. Phát hiện drift là việc của reconciler chứ
  không phải của CI.
- Self-heal: reconcile về desired state trong git, `git diff` rỗng, validator
  PASS lại.

## 5. Sự cố và khôi phục

Xem `incident-drill.log` (nhật ký đầy đủ) và mục tương ứng trong `ANSWERS.md`.

## 6. Promotion và rollback

Xem `rollback-drill.log`: champion v1 → `lab28 release` → v4 → `lab28 rollback`
→ v3. Không sửa một dòng mã nào giữa hai lần.

## Các trang quan sát khi demo

| UI | URL trên máy chạy |
|---|---|
| Envoy | http://localhost:8080/health |
| FastAPI | http://localhost:8100/docs |
| Grafana | http://localhost:3000 |
| Prometheus | http://localhost:9190/targets |
| Jaeger | http://localhost:16686 |
| MLflow | http://localhost:5000 |
| Qdrant | http://localhost:6433/dashboard |
| Airflow | http://localhost:8082 |

Cổng API/Prometheus/Qdrant lệch mặc định vì máy đang chạy sẵn dịch vụ khác —
xem mục "Cổng" trong `ANSWERS.md`.
