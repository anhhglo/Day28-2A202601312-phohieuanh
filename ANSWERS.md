# ANSWERS — Day 28 Track 2, Modern AI Platform Integration Lab

**Hình thức:** làm **cá nhân**. Nhánh làm bài: `ca-nhan-phohieuanh`, đã gộp vào `main`.
**Bộ bằng chứng:** `evidence/` (12 file) và `submission/`.

## 1. Các vai trò đã đi qua

Làm cá nhân nên đi lần lượt đủ năm vai:

| Vai | IP phụ trách | Đã làm gì |
|---|---|---|
| Ingestion & Orchestration | IP01–IP02 | `event_headers`; tạo 4 topic với retention/cleanup theo `contracts.TOPICS`; chạy và đọc log DAG `lab28_ingestion_pipeline` |
| Data & ML | IP03–IP04–IP06 | `dedupe_latest` cho MERGE replay-safe; `feast_online_request` bám `FEATURE_REFS`; release + rollback champion trên MLflow |
| Serving & Retrieval | IP05–IP07 | index hybrid vào Qdrant (ID UUID tất định từ `doc_id`); xác minh vLLM thật — **không đạt**, xem mục 5 |
| Nền tảng & giám sát | IP08–IP10 | Envoy routing + rate limit; 9/10 target Prometheus; dashboard Grafana; trace xuyên gateway → API → Kafka → Airflow → Spark |
| Trình bày / Incident commander | — | diễn tập sự cố có dự đoán trước, `incident-drill.log`; kịch bản demo trong `PROOF.md` |

## 2. Bốn chức năng đã hoàn thiện

Chỉ sửa `src/lab28_platform/integration_tasks.py`, không đụng test.

- **`event_headers` (IP01 + IP10)** — `idempotency-key` luôn có, dạng `bytes`.
  `traceparent` **chỉ** được thêm khi thực sự có trace: một `traceparent` rỗng
  không hợp lệ theo W3C và sẽ làm hỏng ngữ cảnh ở phía consumer, nên thà bỏ hẳn
  header còn hơn gửi chuỗi rỗng. Không hardcode khoá hay mã trace.
- **`dedupe_latest` (IP03)** — duyệt đầu vào **đúng một lần** (đầu vào có thể là
  iterator một chiều từ một lô Kafka), giữ bản có `(occurred_at, event_id)` lớn
  nhất cho mỗi `idempotency_key`, trả về theo thứ tự khoá đã sắp xếp. So sánh cả
  `event_id` để hai bản trùng `occurred_at` không phụ thuộc thứ tự Kafka giao.
- **`feast_online_request` (IP04)** — lấy danh sách feature từ
  `contracts.FEATURE_REFS` thay vì chép lại chuỗi; `full_feature_names=False`.
  Nếu registry đổi tên feature thì chỉ phải sửa một chỗ.
- **`readiness_status` (IP07 + IP08)** — thứ tự ưu tiên rõ ràng: có bất kỳ
  `mandatory=true` hỏng → `not_ready` (thoát sớm); không có bắt buộc hỏng nhưng
  có tuỳ chọn hỏng → `degraded`; còn lại → `ready`.

## 3. `ready` / `degraded` / `not_ready`

- **`ready`** — mọi dependency bắt buộc dùng được. `/ready` trả 200.
- **`degraded`** — phần bắt buộc vẫn tốt, một phần **không** bắt buộc hỏng. Vẫn
  trả 200 nên gateway **giữ** pod trong rotation; câu trả lời có thể kém chất
  lượng (thiếu feature, thiếu ngữ cảnh) nhưng vẫn phục vụ được.
- **`not_ready`** — ít nhất một dependency bắt buộc hỏng. `/ready` trả **503**,
  Envoy loại pod khỏi rotation. Khác `/health`: `/health` chỉ nói tiến trình còn
  sống và **không** chạm vào dependency nào, nên một pod `/health` 200 vẫn có thể
  `/ready` 503.

Phân loại trong lab: Kafka, Qdrant, MLflow là **bắt buộc**; Feast là **không**
bắt buộc (feature nguội làm câu trả lời kém đi, không làm nó sai); vLLM bắt buộc
hay không tuỳ `LAB28_VLLM_REQUIRE_REAL`.

## 4. Sự cố đã tạo — dự đoán, quan sát, nguyên nhân, khôi phục

Nhật ký đầy đủ: `submission/incident-drill.log`.

| Mốc | Hành động | Dự đoán trước | Quan sát thật |
|---|---|---|---|
| T0 | baseline | `degraded` (thiếu vLLM) | `degraded`; Delta `feedback` v16/25 hàng, `documents` v10/20 hàng |
| T1–T2 | `stop feast` | `degraded`, `/ready` **vẫn 200** | HTTP 200, `status=degraded`, `feast ready=False` |
| T3 | `stop qdrant` (bắt buộc) | `not_ready`, `/ready` **503** | HTTP **503**, `status=not_ready` |
| T4–T5 | khởi động lại cả hai | quay về `degraded` | HTTP 200, `status=degraded`, chỉ còn vLLM thiếu |
| T6 | kiểm tra mất dữ liệu | version và số hàng **không đổi** | `feedback` v16/25 hàng, `documents` v10/20 hàng, Qdrant 20 point — **khớp T0** |

**Nguyên nhân:** dừng container có chủ đích. Điểm cần giải thích không phải "vì
sao hỏng" mà là **vì sao hai lần hỏng cho hai trạng thái khác nhau** — đúng cây
quyết định trong `readiness_status`: Feast không bắt buộc nên hệ thống hạ cấp và
vẫn nhận request; Qdrant bắt buộc nên hệ thống tự loại mình khỏi rotation thay vì
trả lời sai.

**Không mất dữ liệu** vì trạng thái nằm ở Delta (log giao dịch trên đĩa) và ở
volume Qdrant, không nằm trong bộ nhớ tiến trình; khởi động lại container đọc lại
đúng version cũ.

## 5. IP07 — vì sao chưa đạt

Máy chạy bài không có GPU. `evidence/ip07-vllm-identity.json` ghi trung thực:
`reachable=false`, `is_real_vllm=false`, `vllm_metric_count=0`. `/ask` trả
**503 `dependency_unavailable`** kèm `trace_id`, không trả lời bịa.

Lab cấm giả lập vLLM, và điều đó đúng: một server chỉ nói được giao thức OpenAI
thì không chứng minh được `/version` là bản dựng vLLM, không có `/v1/models` đúng
model ID, không phát metric tiền tố `vllm:`. Dựng mock sẽ làm 16 test `gpu`/
`langsmith` chuyển xanh mà **không** chứng minh thêm được gì. Chỗ này để
`UNVERIFIED` theo đúng hướng dẫn `SUBMISSION.md`; khi có endpoint vLLM thật từ
Kaggle/máy chung, chỉ cần đặt `LAB28_VLLM_BASE_URL` + `LAB28_VLLM_MODEL_ID` là
chạy tiếp, không phải sửa mã.

## 6. Điều khó nhất

Không phải bốn hàm, mà là một **lỗi của máy chạy** giả dạng lỗi bài làm.

`IT-J2` fail toàn bộ 9 test với `timed out after 300s waiting for Airflow run
... probe returned None`, trong khi `IT-J1` vừa pass ngay trước đó. Đọc log
Airflow thì thấy hai triệu chứng lệch hẳn nhau:

```
jwt.exceptions.ImmatureSignatureError: The token is not yet valid (iat)
... 403 Invalid auth token
run_duration=-578.56691          <-- thời lượng ÂM
```

Đo `time.time()` so với `time.monotonic()`: **CLOCK_REALTIME của WSL2 vọt tiến
+579,6 giây trong khoảng 0,2 s, lặp lại mỗi ~5 s** (~4% số lần đọc rơi vào cú
vọt). Hệ quả kép:

1. Airflow ký JWT thực thi task bằng `time.time()`. Token đúc trúng cú vọt mang
   `iat` ở tương lai ~9,7 phút; api-server đọc đồng hồ sạch nên từ chối là "chưa
   có hiệu lực" → task fail cả 3 lần thử.
2. `dag_run.run_after` cũng bị đóng dấu ở tương lai → DAG run nằm `queued` lâu
   hơn 300 s mà test chờ.

**Cách sửa (không đụng file nào của lab):** neo giờ tường vào `CLOCK_MONOTONIC`
bằng một `sitecustomize.py`, tiêm qua `PYTHONPATH` trong `compose.override.yaml`
(file này đã cho vào `.gitignore`).

Bản vá đầu tiên của tôi **chưa đủ**, và tôi chỉ phát hiện khi đo lại chứ không
phải khi thấy test xanh:

- Nó chỉ vá `time.time()`. Nhưng `datetime.now()` của CPython gọi thẳng đồng hồ
  hệ thống ở tầng C, **không** đi qua `time.time()` — đo lại trong container:
  `time.time()` lệch 0,00 s trong khi `datetime.now()` **vẫn lệch 579,55 s**.
  Airflow đóng dấu `dag_run.run_after` bằng `datetime`, nên nguyên nhân số 2 ở
  trên vẫn còn nguyên; suite xanh hai lần liên tiếp chỉ là chưa gặp xác suất xấu.
- Nó thay `time.localtime` bằng một **hàm Python thuần**. `time.localtime` gốc là
  builtin nên `logging.Formatter.converter = time.localtime` truy cập qua
  `self.converter` không bị bind; hàm Python thuần thì bị bind thành method, nên
  `logging` gọi thành hai đối số và ném
  `TypeError: localtime() takes from 0 to 1 positional arguments but 2 were given`
  mỗi lần MLflow ghi một warning trong container API.

Tôi đã thử vá tiếp `datetime` và **phải bỏ**. Thay `datetime.datetime` bằng một
lớp con đọc giờ đã lọc thì hỏng ở hai chỗ độc lập nhau, cả hai đều đo được:

1. **MLflow treo lúc import.** MLflow suy diễn schema từ type hint pydantic ngay
   khi import; một lớp con của `datetime` làm bước đó treo vô hạn.
   `import lab28_platform.model_registry` chạy 0,7 s khi không vá, treo >60 s khi
   vá; `pytest --collect-only` ăn 100% CPU suốt 51 phút mà không xong.
2. **Airflow kiểm tra kiểu nghiêm ngặt.** `airflow/utils/sqlalchemy.py:158` ném
   `TypeError: expected datetime.datetime, not datetime.datetime(...)` — thông
   báo trông vô lý cho tới khi nhận ra nó so **đúng lớp**, không phải `isinstance`.
   dag-processor crash, `POST /api/v2/.../dagRuns` trả 500.

**Bản vá cuối cùng** vì thế chỉ gồm hai thứ, áp cho container `airflow`, `api`,
`feast` và cho shell chạy pytest/CLI:

- `time.time()` / `time.time_ns()` neo vào `CLOCK_MONOTONIC` — đây là thứ sửa
  được lỗi chí mạng (JWT `iat` ở tương lai làm task fail cả 3 lần thử);
- `time.localtime` / `gmtime` thay bằng **instance của một class có `__call__`**
  thay vì hàm Python thuần, vì instance không phải descriptor nên không bị bind.

Đo lại: `time.time()` lệch **0,00 s** trong cả container Airflow lẫn API,
`logging` hết lỗi, `import model_registry` 0,7 s, host collect đủ 72 test trong
0,62 s.

**Rủi ro còn lại, nêu rõ để không nhận công quá phần đã làm:** `run_after` vẫn
được Airflow đóng dấu bằng `datetime`, nên khoảng **4% số lần trigger DAG** vẫn
có thể bị đẩy về tương lai và nằm `queued` tới 9,7 phút, vượt mốc 300 s mà test
chờ. Không vá được ở tầng Python; sửa gốc cần quyền root trên host WSL2. Cách xử
lý khi gặp: chạy lại chính test đó — bằng chứng là run kẹt trong log
`incident-drill` ban đầu cuối cùng vẫn tự chạy và kết thúc.

Bài học có ba phần. Thứ nhất: khi thông báo lỗi trỏ vào một tầng (ở đây là *xác
thực*) mà mã ở tầng đó không có gì sai, hãy nghi ngờ **nền tảng bên dưới** trước
khi sửa mã — dấu hiệu quyết định là so `run_after` với `date -u`. Thứ hai:
**test xanh không chứng minh bản vá đúng** — bản vá `time.time()` cho 56/56 xanh
hai lần liên tiếp trong khi vẫn để hở một nửa nguyên nhân; chỉ phép đo trực tiếp
mới phân biệt được "đã sửa" với "chưa gặp lại". Thứ ba: **một bản vá hạ tầng phải
được giới hạn ở phạm vi nhỏ nhất có tác dụng, và phải biết dừng.** Bản vá
`datetime` về mặt kỹ thuật thì đúng — nó thật sự khử được cú vọt — nhưng nó phá
hai thư viện lớn, và một bản vá đúng-mà-hỏng-hệ-thống thì tệ hơn là không vá.

## 7. Trade-off đã chọn

- **Dedupe ở tầng Python thay vì trong Spark job.** Chậm hơn một chút và phải giữ
  cả lô trong bộ nhớ, đổi lại quy tắc gộp trùng kiểm thử được **không cần JVM** —
  `tests/test_delta_merge_idempotency.py` chạy trong 2 giây. Với lô lớn thật thì
  nên đẩy xuống `dropDuplicates` phía Spark.
- **`readiness_status` thoát sớm khi gặp lỗi bắt buộc.** Không đọc hết danh sách
  probe, nên phần "chi tiết từng component" trong `/ready` do tầng gọi dựng riêng.
  Đổi lại quyết định `not_ready` là O(1) trên đường nóng.
- **Giữ rate limit 10 rps của Envoy khi seed.** Ban đầu `seed --via-gateway` bị
  429 hàng loạt. Có thể nới rate limit cho seed chạy trót lọt, nhưng như thế là
  làm hỏng chính thứ IP08 phải chứng minh. Chọn: nạp dữ liệu **trực tiếp** vào
  API (13 documents + 12 feedback, 0 rejected) và giữ đường gateway ở nhịp thấp
  để 429 vẫn là bằng chứng thật.
- **Không dựng vLLM giả.** Đánh đổi 16 test xanh lấy một bộ bằng chứng trung
  thực. Xem mục 5.
- **Vá đồng hồ bằng `PYTHONPATH` thay vì sửa `compose.yaml`.** Bỏ
  `compose.override.yaml` đi là repo chạy đúng cấu hình gốc; `docker compose -f
  compose.yaml --env-file ports.template --profile full config` vẫn sạch.

## 8. Sẽ cải tiến khi triển khai thật

1. **Idempotency phải có TTL và nơi lưu.** Hiện chống trùng dựa vào MERGE key ở
   Delta — đúng nhưng chỉ chặn được sau khi đã đi hết pipeline. Nên có một chốt
   chặn ở tầng API (Redis SETNX theo `idempotency-key`) để lần gửi lại thứ hai
   trả luôn kết quả cũ thay vì đi thêm một vòng Kafka → Airflow → Spark.
2. **`max_active_runs=1` sẽ là nút thắt.** Hợp lý cho lab; production cần phân
   vùng theo partition/tenant để các lô độc lập chạy song song.
3. **Trace chưa liền mạch qua đường `/ask`.** `ip10-trace.json` còn thiếu
   `lab28.qdrant.query`, `lab28.feast.get_online_features`. Cần bổ sung
   instrumentation cho client Qdrant/Feast để một trace phủ **cả** đường ghi lẫn
   đường đọc.
4. **SLO thật thay cho alert nhị phân.** Hiện `Lab28ApiUnavailable` chỉ bắt trạng
   thái "không scrape được". Nên có error budget trên tỷ lệ 5xx và p95 độ trễ —
   p99 852 ms của `/ready` là do probe gọi thẳng dependency, production nên cache
   kết quả probe vài giây.
5. **Đồng hồ là một dependency.** Bài học lớn nhất buổi này: nên có một check
   `clock_skew` trong `/ready` (so `time.time()` với `time.monotonic()` qua một
   cửa sổ dài hơn một cú vọt), vì đồng hồ lệch làm mọi tầng khác báo lỗi sai chỗ.
6. **Cache kết quả probe của `/ready`.** Đo được nút thắt: `/health` p50 5,6 ms
   trong khi `/ready` p50 307 ms, và 387/588 ms thời gian probe là do MLflow tải
   lại artifact ở **mỗi** lần gọi để phân giải alias `champion`. Alias đổi theo
   phút chứ không theo request, nên một cache 5–10 giây kèm single-flight sẽ kéo
   `/ready` về gần `/health`. Quét mức đồng thời cho thấy hệ thống hết headroom
   quanh 8 worker (p50 308 → 422 → 806 ms khi lên 8 → 16 → 32). Chi tiết trong
   `submission/load-profile.txt`.

## 9. Ghi chú môi trường (không thuộc bài làm)

Máy chạy bài đã có sẵn các dịch vụ khác chiếm cổng 6333, 8000, 9090, 9091 nên
dùng file `ports.local` (đã gitignore, **không chứa mật khẩu**) để đổi cổng
**phía host**: API 8100, Prometheus 9190, Qdrant 6433, Pushgateway 9191. Cổng
trong mạng Docker giữ nguyên. Mọi lệnh trong `PROOF.md` dùng
`--env-file ports.local` thay cho `ports.template`.

`ports.local` và `compose.override.yaml` **không** được nộp: chúng là cấu hình
riêng của máy chạy, và bỏ chúng đi thì repo chạy đúng như cấu hình gốc của lab.
