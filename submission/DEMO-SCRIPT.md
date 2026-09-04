# Kịch bản demo — Day 28 Track 2

Mỗi bước ghi rõ **lệnh**, **thứ chỉ ra trên màn hình**, và **câu trả lời cho
"vừa chứng minh được điều gì"**. Không chiếu màn hình xanh mà không giải thích.

Chuẩn bị trước khi vào lớp:

```text
docker compose -f compose.yaml -f compose.gpu.yaml --env-file ports.template \
  --profile full --profile gpu up -d --wait
uv run lab28 topics && uv run lab28 index --source file && uv run lab28 release
```

---

## Phút 0–1 — Bài toán và ba vùng kiến trúc

Mở `docs/images/lab28-architecture-overview.png`. Nói đúng ba câu:

1. **Luồng chính:** người dùng → Envoy → FastAPI → Kafka → Airflow → Delta Lake.
2. **Dữ liệu và mô hình:** Delta nuôi Feast (feature), Qdrant (vector), MLflow
   (registry); FastAPI gọi vLLM để sinh câu trả lời.
3. **Giám sát:** Prometheus/Grafana cho số liệu, OTel/Jaeger cho một trace xuyên
   suốt.

Ownership: `contracts/integration-matrix.yaml` gán mỗi IP cho một vai
(`team-ingestion`, `team-data`, `team-serving`, `team-platform`). Làm cá nhân nên
tôi đi qua cả năm vai — bảng phân vai ở mục 1 `ANSWERS.md`.

---

## Bước 1 — Luồng chạy đúng, bám theo MỘT trace ID

```text
uv run lab28 seed --via-gateway --limit 4
uv run pytest integration-tests/test_j1_golden_path.py -q
```

Chỉ ra trong `evidence/ip01-kafka-consume.json`: `traceparent` và
`idempotency-key` nằm trong **header Kafka**, không phải trong body.

> **Chứng minh gì:** ngữ cảnh trace sống sót qua ranh giới bất đồng bộ. Đây là
> chỗ trace hay đứt nhất, vì Kafka không tự truyền context như HTTP.

Rồi mở Jaeger, dán trace ID từ `evidence/ip10-trace.json`.

> **Chứng minh gì:** một request duy nhất hiện thành chuỗi span qua gateway →
> API → Kafka → Airflow → Spark → Feast/Qdrant → vLLM.

---

## Bước 2 — Replay không tạo bản ghi trùng

```text
uv run pytest integration-tests/test_j2_idempotent_replay.py -q
```

Mở `evidence/ip03-delta-history.json`: mọi `operation` đều là `MERGE`.

> **Chứng minh gì:** Kafka **giữ đủ** mọi lần giao (mỗi lần một `event_id`), còn
> Delta chỉ có **một** hàng cho mỗi `idempotency_key`. Hai điều đó không mâu
> thuẫn: broker ghi lại sự kiện, lakehouse ghi lại **sự thật**.

Câu hay bị hỏi: *"version Delta tăng mà số hàng không tăng thì có sai không?"*
→ Không. MERGE tạo commit mới kể cả khi chỉ cập nhật tại chỗ. Test khẳng định
đúng điều này — số hàng mới là bất biến, không phải số version.

---

## Bước 3 — Sự cố: dự đoán trước, rồi mới làm

Nói **trước** dự đoán, sau đó mới chạy:

```text
docker compose --env-file ports.template stop feast     # không bắt buộc
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8080/ready
docker compose --env-file ports.template stop qdrant    # bắt buộc
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8080/ready
```

Dự đoán: **200 / degraded**, rồi **503 / not_ready**. Nhật ký thật ở
`submission/incident-drill.log`.

> **Chứng minh gì:** `/ready` không phải nhị phân. Feast nguội làm câu trả lời
> **kém đi**; Qdrant chết làm câu trả lời **sai**. Chỉ cái thứ hai mới đáng để
> hệ thống tự loại mình khỏi rotation.

Khôi phục rồi so số hàng Delta với lúc đầu — trùng khít, không mất dữ liệu.

---

## Bước 4 — Alert thật sự kêu

```text
docker compose --env-file ports.template stop api
# đợi ~40s (rule Lab28ApiUnavailable có for=30s)
```

Mở Prometheus `/alerts`: `Lab28ApiUnavailable` chuyển `pending` → `firing`.

> **Chứng minh gì:** alert này *actionable* — nó có `for=30s` để không kêu vì
> một lần scrape trượt, có `severity=critical`, và annotation chỉ thẳng việc
> phải làm. Alert không có ngưỡng thời gian thì chỉ là tiếng ồn.

---

## Bước 5 — Hàng lỗi và replay có kiểm soát

```text
uv run lab28 dlq
uv run lab28 dlq --replay --limit 5
```

> **Chứng minh gì:** bản tin hỏng không làm nghẽn pipeline mà rơi vào
> `data.raw.dlq` kèm nguyên văn payload. Replay là **hành động của người vận
> hành**, không tự động — vì bản tin bị dead-letter do lỗi phần mềm sẽ quay lại
> ngay lập tức nếu lỗi chưa được sửa.

---

## Bước 6 — Đổi phiên bản mô hình rồi quay lui

```text
uv run lab28 release
uv run lab28 inspect | grep -A3 mlflow
uv run lab28 rollback
uv run lab28 inspect | grep -A3 mlflow
```

> **Chứng minh gì:** champion đổi và quay lui **không sửa một dòng mã nào**.
> Alias là con trỏ; deploy là việc di chuyển con trỏ, không phải build lại.

Nhật ký: `submission/rollback-drill.log`.

---

## Bước 7 — GitOps: desired state, drift, self-heal

```text
uv run python scripts/validate_manifests.py
```

Mở `submission/gitops-drill.log`.

> **Chứng minh gì:** điểm đáng nói nhất là khi inject drift `replicas 2→9` thì
> validator **vẫn PASS** — manifest lệch nhưng không sai. Phát hiện drift là
> việc của reconciler chứ không phải của CI. `targetRevision` trỏ vào một tag
> bất biến, nên rollback là trỏ về tag trước.

---

## Kết luận — một trade-off và một điều sẽ cải tiến

**Trade-off:** giữ nguyên rate limit 10 rps của Envoy khi seed, chấp nhận phải
nạp dữ liệu qua đường trực tiếp. Nới rate limit thì seed chạy trót lọt nhưng
phá đúng thứ IP08 phải chứng minh.

**Sẽ cải tiến:** cache kết quả probe của `/ready`. Đo được `/health` p50 5,6 ms
so với `/ready` p50 307 ms, trong đó MLflow chiếm 387/588 ms vì tải lại artifact
ở **mỗi** lần phân giải alias. Alias đổi theo phút chứ không theo request.

---

## Câu hỏi hay gặp

**Vì sao dedupe ở Python mà không ở Spark?**
Delta MERGE lỗi khi source có hai hàng khớp cùng một hàng target, nên lô phải
duy nhất trước khi tới writer. Đặt ở Python khiến quy tắc kiểm thử được **không
cần JVM** — `tests/test_delta_merge_idempotency.py` chạy 2 giây. Với lô lớn thật
thì nên đẩy xuống `dropDuplicates` phía Spark; đây là đánh đổi có ý thức giữa
tốc độ phản hồi khi phát triển và khả năng mở rộng.

**Vì sao so `(occurred_at, event_id)` chứ không chỉ `occurred_at`?**
Hai bản tin cùng mốc thời gian là chuyện thường. Nếu chỉ so `occurred_at` thì
bản nào thắng phụ thuộc thứ tự Kafka giao — cùng dữ liệu, chạy hai lần ra hai
kết quả. Thêm `event_id` làm khoá phụ khiến kết quả chỉ phụ thuộc **nội dung lô**.

**`/health` và `/ready` khác gì nhau?**
`/health` chỉ nói tiến trình còn sống và **không chạm** dependency nào — nếu nó
gọi database thì database chậm sẽ khiến Kubernetes giết pod đang khoẻ. `/ready`
mới trả lời "nhận request được chưa". Một pod `/health` 200 mà `/ready` 503 là
trạng thái hoàn toàn hợp lệ.

**Nếu vLLM chết giữa lúc đang phục vụ thì sao?**
`/ready` chuyển `not_ready` (vLLM là mandatory khi `REQUIRE_REAL=true`), Envoy
rút pod khỏi rotation, và `/ask` trả **503 `dependency_unavailable`** kèm
`trace_id` — không bịa câu trả lời. Ghi lại được ở
`evidence/ip07-vllm-identity.json` lúc chưa nối GPU.

**Làm sao biết đó là vLLM thật chứ không phải server giả OpenAI?**
Ba dấu hiệu cùng lúc: `/version` trả bản dựng vLLM, `/v1/models` có đúng model
ID đã cấu hình, và `/metrics` phát các series tiền tố `vllm:`. Một mock đáp ứng
được giao thức chat completion nhưng không dựng được cả ba.

**Chỗ nào sẽ vỡ trước khi lên production?**
`max_active_runs=1` của Airflow. Hợp lý cho lab vì nó làm mọi lần chạy tuần tự
và dễ giải thích, nhưng là nút thắt ngay khi có nhiều tenant. Cần phân vùng theo
partition/tenant để các lô độc lập chạy song song.
