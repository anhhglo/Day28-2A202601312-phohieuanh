# Bằng chứng chạy thật — Day 28 Track 2

Chạy trên WSL2 (Ubuntu, 20 CPU, 23,5 GB RAM, **NVIDIA RTX 5080 16 GB**),
`docker compose -f compose.yaml -f compose.gpu.yaml --profile full --profile gpu`,
**15 service** `running`/`healthy`. Mọi ID dưới đây lấy từ `evidence/*.json` do
`uv run lab28 evidence` và bộ integration test sinh ra, không chép tay.

## Điểm số tổng hợp

`evidence/integration-report.json` — **score 100/100**, `ready: true`,
6/6 điểm probe được đều `ready`, `pillars: {operations: 1.0, reliability: 1.0}`.
4 điểm còn lại (IP02, IP08, IP09, IP10) được đánh dấu *unverified-from-process*
theo thiết kế — tiến trình phục vụ không tự chứng minh được chúng — nên mỗi điểm
có một file evidence riêng bên dưới.

Kiểm thử: **71 passed, 0 failed** (`integration-tests -m "not langsmith"`, 6 phút).
Test duy nhất bị bỏ qua là nhóm `langsmith`, vì lớp không cấp `LANGSMITH_API_KEY`.
Phần mã: 4 + 83 test, ruff sạch, `verify_matrix` 245 checks, `check_portability`,
`validate_manifests` đều rc 0.

## 1. Luồng chạy đúng (J1 golden path)

| Bước | Bằng chứng | Giá trị |
|---|---|---|
| IP08 client → Envoy → API | `ip08-gateway.json` | `x-request-id=248205ae-9782-99f6-897d-5ecd61fdbf98`, 30 request → 9×200 + 21×429 |
| IP01 API → Kafka | `ip01-kafka-consume.json` | `data.raw` p0 offset 95, `idempotency-key=it-j1-67162cea`, header `traceparent` có mặt |
| IP02 Kafka → Airflow | `ip02-airflow-run.json` | dag_run `it-d0f4dad0` = `success`, 4 asset event |
| IP03 Airflow → Delta | `ip03-delta-history.json` | `feedback` v63, `documents` v31, toàn bộ operation là `MERGE` |
| IP04 Delta → Feast | `ip04-feast-online.json` | entity `asker_id=it-j1-67162cea`, `delta_version=57`, mọi feature `PRESENT` |
| IP05 Delta → Qdrant | `ip05-qdrant-search.json` | 37 point, hybrid search trả `doc-ip09-metrics` score 0,833 |
| IP06 Delta → MLflow | `ip06-mlflow-release.json` | `lab28-rag-release v12 is champion` (sau khi diễn tập rollback) |
| IP07 API → vLLM | `ip07-vllm-identity.json` | `version=0.28.0`, `served_models=['Qwen/Qwen3-1.7B']`, **111 metric `vllm:`**, `is_real_vllm=true` |
| IP10 trace xuyên luồng | `ip10-trace.json` | trace `77291f33b2f7426fbbcfd522cf67ac96`, **26 span**, **4 service**, `required_spans_missing: []` |

Một trace duy nhất đi qua `lab28-gateway` → `lab28-api` → `lab28-airflow` →
`lab28-vllm`, mang đủ chuỗi span: `lab28.gateway.request`, `lab28.api.ingest`,
`lab28.kafka.produce`, `lab28.kafka.consume`, `lab28.airflow.dag`,
`lab28.spark.delta_merge`, `lab28.api.ask`, `lab28.feast.get_online_features`,
`lab28.qdrant.query`, `lab28.vllm.chat_completion`, và `llm_request` do chính
vLLM phát ra.

## 2. Replay-safe (J2)

`test_j2_idempotent_replay.py` → **9 passed**. Gửi lại cùng một lô thì Kafka giữ
đủ mọi lần giao (mỗi lần một `event_id`), nhưng Delta chỉ có **một** hàng cho mỗi
`idempotency_key`, Feast đếm sự kiện **một** lần, Qdrant giữ **đúng một** point
cho mỗi tài liệu. Delta version *được phép* tăng dù số hàng không đổi — đó là
đặc tính của MERGE và test khẳng định đúng điều đó.

Cơ chế: `dedupe_latest` gộp lô theo `idempotency_key` trước khi vào MERGE (Delta
lỗi nếu source có hai hàng khớp cùng một hàng target), chọn bản có
`(occurred_at, event_id)` lớn nhất nên kết quả không phụ thuộc thứ tự Kafka giao.

## 3. Hàng lỗi và replay có kiểm soát (IP02)

`dlq-drill.log` — hai loại bản tin trong `data.raw.dlq`:

- một bản **hỏng JSON** (`error_category: validation`, offset 103);
- một bản hợp lệ bị park có chủ đích (`attempts: 3`, còn nguyên `traceparent`).

`lab28 dlq --replay --limit 5` → **`{"replayed": 1, "skipped": 4}`**. Đúng điều
cần chứng minh: replay **từ chối** tái nạp payload nó không đọc được, thay vì đẩy
bản tin hỏng quay lại vòng lặp. Replay là hành động của người vận hành, không
tự động.

## 4. Metrics và alert (IP09)

`ip09-prometheus-targets.json` — **10/10 target `up`**, gồm cả `lab28-vllm`.
`ip09-grafana-dashboards.json` — dashboard `Lab 28 Platform Overview`.

`alert-drill.log` chứng minh alert *thật sự kêu*, không chỉ tồn tại trong file:

| Thời điểm | Trạng thái `Lab28ApiUnavailable` |
|---|---|
| bình thường | không active |
| +24 s sau khi dừng API | `pending` |
| +49 s | **`firing`** |
| sau khi khôi phục | tự tắt |

Đó là một alert *actionable*: có `for=30s` nên không kêu vì một lần scrape trượt,
có `severity=critical`, và annotation chỉ thẳng việc phải làm.

## 5. Kubernetes / GitOps

`gitops-drill.log` — máy không có cluster nên phần này chứng minh ở tầng
**desired state**, thứ Argo CD thực sự đối chiếu, và nói rõ là không giả lập
output của cluster:

- `validate_manifests.py` PASS: đủ 9 kind bắt buộc, Deployment chạy non-root.
- `gitops/application.yaml`: `targetRevision: refs/tags/v3.0.0` (tag bất biến →
  rollback là trỏ về tag trước), `selfHeal: true`, `prune: true`,
  `revisionHistoryLimit: 5`.
- Drift injected `replicas: 2 → 9`. Điểm đáng nói: validator **vẫn PASS** —
  manifest lệch nhưng không sai. Phát hiện drift là việc của reconciler, không
  phải của CI.
- Self-heal: reconcile về desired state, `git diff` rỗng, validator PASS lại.

## 6. Sự cố và khôi phục — đủ ba trạng thái

`incident-drill.log`. Mỗi bước nói dự đoán **trước** khi chạy:

| Mốc | Hành động | Dự đoán | Quan sát |
|---|---|---|---|
| T0 | baseline | `ready` | `ready`; feedback v63/73 hàng, documents v31/37 hàng, Qdrant 37 point |
| T1 | dừng **Feast** (không bắt buộc) | `degraded`, gateway **vẫn 200** | api 200, gateway 200, `status=degraded` |
| T2 | `/ask` khi degraded | vẫn trả lời, tự khai lý do | 1089 ký tự + 3 nguồn, `degraded=true`, lý do `feature store unavailable` |
| T3 | dừng thêm **Qdrant** (bắt buộc) | `not_ready`, gateway **eject pod** | api 503, gateway 503 **không có `components`** → 503 của chính Envoy |
| T4–T5 | bật lại cả hai | về `ready` | api 200, gateway 200, `status=ready` |
| T6 | kiểm tra mất dữ liệu | version và số hàng **không đổi** | v63/73, v31/37, 37 point — **khớp T0** |

Chi tiết đáng chú ý ở T3: thân phản hồi phân biệt hai loại 503. 503 của **ứng
dụng** có `components`; 503 của **gateway** thì không. Chỉ khi gateway trả loại
thứ hai mới chứng minh pod đã thực sự ra khỏi rotation.

## 7. Promotion và rollback

`rollback-drill.log`: champion đổi và quay lui **không sửa một dòng mã nào**.
Alias là con trỏ; deploy là việc di chuyển con trỏ, không phải build lại.

## 8. Load và nút thắt

`load-profile.txt`. Tóm tắt: `/health` p50 5,6 ms so với `/ready` p50 307 ms —
nút thắt là probe dependency đồng bộ, trong đó MLflow chiếm 387/588 ms vì tải
lại artifact ở mỗi lần phân giải alias. Đường `/ask` thật: p50 1334 ms ở 1
worker, p95 **giảm** khi lên 4 worker nhờ continuous batching của vLLM.

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
| vLLM | http://localhost:8001/v1/models |

Cổng API/Prometheus/Qdrant lệch mặc định vì máy chạy sẵn dịch vụ khác — xem mục
9 trong `ANSWERS.md`.
