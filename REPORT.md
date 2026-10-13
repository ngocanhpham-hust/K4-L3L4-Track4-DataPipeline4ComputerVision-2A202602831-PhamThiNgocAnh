# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Phạm Thị Ngọc Anh |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602831` |
| GitHub | `ngocanhpham-hust` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `trip-04868` |

> Cách tính trip: MSSV có chữ `A` nên `int("2A202602831")` báo lỗi. Em lấy phần chữ số `2202602831 % 26 = 13` → `list-trips[13] = trip-04868` (cũng là `silver_frames` map index 13 trong Airflow).

## Kết quả cuối

- `dataset_version` của bạn: `f33c79710106`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: **732 / 132 / 156** (tổng 1020 khung; 18 / 4 / 4 trip)
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, `fail_task_log.png`, `airflow_retry_graph.png`.
- Đầu ra đầy đủ của từng bước được lưu trong `report/step_outputs/`.

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Hai dòng đầu ở bronze (`report/step_outputs/step1_bronze.txt`) cho thấy:

| | Nguồn A (`raw_telemetry_a`) | Nguồn B (`raw_telemetry_b`) |
|---|---|---|
| Thời gian | `ts_utc` chuỗi ISO UTC `2025-10-07T08:00:00.014Z` | `timestamp_ms` epoch ms `1759933801509` |
| Tốc độ | `speed_mps` = 1.213 (m/s) | `spd_kmh` = 55.11 (km/h) |
| Hướng, vị trí | `heading_deg`, `lat`, `lon`, `alt_m` | `hdg`, `latitude`, `longitude`, `altitude` |
| Gia tốc, lái | `acc_x`, `acc_y`, `steer` | `ax`, `ay`, `steering` |
| Bàn đạp | `gas`, `brake` dạng tỉ lệ 0..1 (0.028) | `throttle_pct`, `brake_pct` dạng % (2.0) |
| Đồng hồ | `telemetry_clock_offset_ms = 0` | `telemetry_clock_offset_ms = 1500` (đồng hồ thiết bị chạy nhanh hơn video 1,5 s) |

Bronze ghi nguyên bản và chỉ thêm. Nếu gộp và ép kiểu ngay thì một quy tắc chuyển đổi sai, ví dụ quên chia 3.6, sẽ phá dữ liệu gốc và không có gì để chạy lại. Giữ nguyên chữ (kể cả ô trống `''` của tốc độ thiếu) giúp không mất dòng nào khi parse lỗi, và mọi sửa đổi đều làm lại được từ bronze. Mỗi nguồn một bảng vì hai schema khác nhau: ép chung một bảng sẽ phải đoán tên cột và đơn vị ngay ở bước ingest. Bằng chứng ingest lặp không nhân đôi: lần nạp 2 báo `already present`, `history bronze.raw_telemetry_a` chỉ có **1 commit** (version 0, 3021 dòng), `bronze_video_files` có 2 dòng với 2 sha256 khác nhau.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

`stg_telemetry_b.sql` đổi tên cột về schema của A, chia `spd_kmh / 3.6` để ra m/s, chia `throttle_pct/brake_pct` cho 100, và đổi `timestamp_ms` thành timestamp bằng `to_timestamp((timestamp_ms - offset_ms)/1000)`, trong đó `offset_ms` lấy từ `bronze.video_files` (1500 ms). Sau đó `silver_telemetry_classified` hợp nhất A và B bằng `union all by name`. Bằng chứng: dòng đầu của B thô là `1759933801509` = 14:30:01.509 UTC; sau khi trừ 1,5 s thì `bat_dau` của B = **2025-10-08 14:30:00.009**, khớp thời điểm bắt đầu video (14:30:00). Tốc độ tối đa sau chuẩn hóa là 71 km/h (A) và 100 km/h (B), đều hợp lý.

| drive | duplicate | speed_missing | speed_out_of_range | tổng cách ly | sạch | nhận về |
|---|---|---|---|---|---|---|
| drive_a | 20 | 30 | 3 | 53 | 2968 | 3021 |
| drive_b | 20 | 30 | 3 | 53 | 3181 | 3234 |

Sạch + cách ly = nhận về ở cả hai drive, nên không dòng nào biến mất im lặng. Dòng lỗi vào `silver_telemetry_rejects` kèm `reject_reason`, `_batch_id` và `_row_no`. Nhờ vậy có thể đếm lỗi theo nguồn, truy về dòng CSV gốc, báo cho đội phần cứng, và khôi phục nếu luật làm sạch sai. Xóa thẳng thì mất cả bằng chứng lẫn khả năng sửa.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

`max_gap_ms` báo **WARN 2**: mỗi drive có một khoảng mất tín hiệu hơn 1 s (A: 3073 ms, B: 3124 ms). Đây là lỗi đã biết và chấp nhận được: những dòng còn lại vẫn đúng, còn các khung rơi vào khoảng trống bị gắn `sync_lost` và loại ở gold (7 khung mỗi drive). Dừng cả pipeline vì lỗi này thì được ít mà mất nhiều. `in_range` (0..70 m/s) là `error` vì tốc độ trên 252 km/h là dữ liệu hỏng chắc chắn; để nó đi tiếp sẽ làm bẩn nhãn huấn luyện, nên phải chặn. Trong `break_gate.json`, khi nới `max_speed_mps` lên 1000 thì 6 dòng vô lý (3 dòng mỗi drive) lọt qua bước phân loại vào silver. Test `in_range_silver_telemetry_speed_mps__70__0` bắt đúng **6 dòng**, kết quả `PASS=5 WARN=1 ERROR=1`. Sau khi khôi phục thì về `PASS=6 WARN=1 ERROR=0`. Bài học: cổng chất lượng là lớp phòng thủ độc lập với luật làm sạch. Khi ai đó vô tình nới luật, test vẫn bắt được, và trong DAG thì `dbt_test_silver` lỗi sẽ chặn `dbt_run_gold`, nên dữ liệu xấu không tới gold hay warehouse.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

Chạy lại partition `trip-00003` (60 khung) hai lần: trước **1246 dòng, vân tay `cb6118dbb0b8`**; sau **1246 dòng, vân tay `cb6118dbb0b8`**, không đổi. Cũng dữ liệu đó mà ghi `append` hai lần thì ra **120 dòng cho một trip 60 khung**, tức nhân đôi 2x. `history silver.frames` cho thấy hai lần chạy lại là version 26 và 27, mỗi version `WRITE ... predicate trip_id = 'trip-00003'`, `rows_added 60`. Mỗi lần ghi là **một commit nguyên tử**: vừa thay các dòng cũ của trip, vừa thêm dòng mới, trong cùng một entry của transaction log. Nếu tiến trình chết giữa chừng thì commit không được ghi, người đọc vẫn thấy version trước, đầy đủ. Còn "xóa rồi ghi" là hai thao tác riêng: chết sau bước xóa thì trip đó mất dữ liệu, chết khi đang ghi thì còn một nửa. Ngoài ra, version cũ vẫn còn để time travel hoặc rollback.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

Khung **đen** có độ sáng trung bình dưới 12 (`dark_luma`): camera hỏng hoặc bị che, không có thông tin gì, nên bị **loại khỏi gold** (A: 6, B: 4). Khung **tối** có độ sáng từ 12 đến dưới 40 (`dark_soft`): đường hầm, bóng râm, chiều tối. Đây là điều kiện thật mà mô hình phải học, nên chỉ **gắn cờ `is_dark` và giữ lại** (A: 4, B: **61**). Em đồng ý, vì bỏ khung tối sẽ làm dataset thiên về trời sáng và mô hình kém ở hầm. Tuy vậy, nên theo dõi tỉ lệ khung tối trong từng split để val/test cũng có đủ cảnh tối.

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 60 | 8 |
| 2 | 149 | 52 |
| 6 | 363 | 244 |

Em chọn **ngưỡng 2** (đang dùng). Ngưỡng 0 chỉ bắt khung giống hệt bit, nên bỏ sót khung gần trùng do nhiễu nén. Ngưỡng 6 loại 363/602 khung của A (60%) và 244/644 của B, tức là xóa cả những khung khác nhau thật. Ngưỡng 2 bắt được camera đứng hình ở A (verify: 11/12 khung) mà vẫn giữ phần lớn dữ liệu. Ngưỡng phụ thuộc tốc độ: lấy mẫu 2 khung/giây, xe chạy chậm (A có trip 6.5 km/h, 17.6 km/h) thì hai khung liền nhau gần như giống hệt, nên A có 149 khung trùng so với 52 của B (B chạy khoảng 70 km/h). Cách tốt hơn là lấy mẫu theo quãng đường thay vì theo thời gian, hoặc nới ngưỡng khi tốc độ thấp.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

Với 1020 khung đáng dùng thuộc 26 trip: chia ngẫu nhiên **theo khung hình** (70/15/15) làm **26/26 trip** nằm ở nhiều hơn một split, và cả **26 trip** đều có khung ở cả train lẫn test. Chia **theo trip** (gold của pipeline) thì **0 trip** bị chia cắt; test `no_straddle` của dbt cũng PASS. Hai khung liền nhau chỉ cách 0,5 s, cùng đường, cùng xe phía trước, cùng ánh sáng. Nếu khung t nằm ở train và khung t+0,5 s nằm ở test, mô hình chỉ cần "nhớ cảnh" là đúng, nên điểm test đo khả năng ghi nhớ chứ không đo khả năng tổng quát hóa sang con đường mới. Khi triển khai, xe luôn gặp chuyến mới, và điểm thật sẽ thấp hơn nhiều so với con số đẹp giả tạo đó.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

Băm nội dung nghĩa là cùng nội dung thì ra cùng version, khác nội dung thì version khác. Version theo ngày giờ hay số đếm sẽ đổi mỗi lần chạy dù dữ liệu không đổi, khiến không biết hai mô hình có train trên cùng dữ liệu hay không, và cache hay so sánh thí nghiệm đều vô nghĩa. Với em: `step 4` cho `f33c79710106`, `export-dataset` lần hai cũng cho `f33c79710106` (cùng `manifest_sha256 b676b530…`). Lần `step all --reset` đầu tiên và DAG Airflow cũng ra đúng version này. Đáng chú ý: sau thí nghiệm idempotent, `silver.frames` đã lên Delta version 27 nhưng nội dung không đổi, nên `dataset_version` vẫn giữ nguyên. Version của em khác bạn bè vì `gold_dataset_frames.sql` xếp trip theo `md5(trip_id || LAB_STUDENT_ID)`. MSSV `2A202602831` cho một thứ tự trip riêng, nên trip nào vào train/val/test (18/4/4 trip, 732/132/156 khung) khác nhau, cột `split` khác, và hash khác.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

Theo `lake/gold/datasets/f33c79710106/manifest.json` (bản đầy đủ ở `report/step_outputs/step4_manifest_lineage.txt`): dataset được tạo từ `bronze.raw_telemetry_a` v0, `bronze.raw_telemetry_b` v0, `bronze.trips_raw` v0, `bronze.video_files` v1, `silver.frames` v27, `silver.telemetry` v0, `silver.frames_flagged` v0, `silver.frame_telemetry` v0. Hai video đầu vào có sha256 `636ba1a9…e7e9aa` (drive_a) và `bfa62703…ee9fdc` (drive_b). Luật áp dụng: loại `is_black`, `is_blurry`, `is_near_dup`, `not sync_ok`; split "per trip, stratified by drive, md5 order". Kết quả là 1020 khung (A 436, B 584). Ba tháng sau, để tái tạo cần: sha256 của video để chắc đúng file gốc, số version Delta để time travel đúng trạng thái bảng, `rules`, MSSV dùng làm salt, và `dataset_version`/`manifest_sha256` để đối chiếu kết quả. Manifest còn thiếu commit Git của code và các `vars` dbt (`dup_hamming=2`, `sync_tol_ms=150`…). Nên ghi thêm hai thứ này, vì đổi một ngưỡng là ra dataset khác.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

Đặt `LAB_FAIL_ONCE=trip-04868`, trigger `day09_reset` rồi `day09_video_pipeline` (run `run_2_fail_once`). Kết quả: **1 task đỏ** là `silver_frames` map index 13, log có `simulated crash while extracting trip-04868` (ảnh `fail_task_log.png`). **7 task `upstream_failed`**: `dbt_run_silver`, `dbt_test_silver`, `dbt_run_gold`, `dbt_test_gold`, `publish_lake`, `export_dataset`, `publish_warehouse`. **29 task success**: 25 trip còn lại, 3 task bronze và `trip_commands`. Em chỉ **clear task đỏ đó** (kèm downstream), nên chỉ 1 + 7 = 8 task chạy lại; 25 trip đã xong không phải giải mã video lại. DAG chuyển xanh (ảnh `airflow_retry_graph.png`). Sau đó `scripts/lab.sh verify` đạt **27/27**, `dataset_version` vẫn là **`f33c79710106`** như trước khi mô phỏng lỗi, và gold vẫn 1020 khung. Không nhân đôi vì `extract_trip` ghi overwrite theo `trip_id`, và lần chạy hỏng chết trước khi commit nên không để lại dòng dở dang nào. Bằng chứng trong `history silver.frames` (`report/step_outputs/step5_after_clear_verify.txt`): trip-04868 chỉ có **một** commit (version 25, `rows_added 60`, `predicate trip_id = 'trip-04868'`), chính là lần chạy lại sau khi clear.

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

1. **Giải mã video và trích khung** (FFmpeg trong một container Airflow, `parallelism=2`, `max_active_tis_per_dagrun=1`): 2 drive khoảng 10 phút video đã mất khoảng 1,5 phút; 500 xe, mỗi xe vài giờ và nhiều camera, sẽ tốn hàng nghìn giờ CPU mỗi ngày. Em sẽ thay bằng worker phân tán (KubernetesExecutor hoặc Celery, hay Ray/Spark), giải mã bằng GPU (NVDEC), và lọc hoặc lấy mẫu ngay trên xe (edge) trước khi upload.
2. **DuckDB một file + dbt-duckdb trên một node**: chỉ một tiến trình ghi được, bộ nhớ một máy. Em sẽ chuyển sang engine phân tán đọc thẳng Delta trên object storage (Spark, Databricks hay Trino), xử lý tăng dần theo partition ngày/xe thay vì build lại toàn bảng.
3. **Một task Airflow cho mỗi trip, chạy theo lô thủ công**: 500 xe × hàng chục trip = hàng chục nghìn mapped task mỗi ngày, scheduler và metadata DB sẽ quá tải. Em sẽ dùng ingest hướng sự kiện (Kafka hoặc S3 event khi xe upload), gom partition theo xe-ngày, và chạy backfill theo ngày. Kèm theo đó: MinIO một node thay bằng S3 hoặc object store phân tán có lifecycle (đẩy bronze video sang lớp lưu trữ lạnh), và Postgres thay bằng kho dạng cột (ClickHouse, BigQuery) cho dashboard.

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

**Câu hỏi 1: trip nào mất nhiều khung nhất khi vào gold?** (thẻ "Câu hỏi riêng 1" trên dashboard)

```sql
select trip_id, drive_id, lighting, conditions, split, frames, frames_kept,
       round(100.0*(frames-frames_kept)/frames,1) as pct_dropped, avg_speed_kmh
from marts.mart_trip_summary order by pct_dropped desc, trip_id limit 10
```

Top 5: `trip-04622` mất 56.7% (26/60 khung giữ lại, 67 km/h), `trip-03904` mất 50.0%, `trip-02086` mất 46.7% (17.6 km/h), `trip-08888` mất 40.0% (**6.5 km/h**), `trip-07774` mất 38.2% (12.8 km/h). **8/10 trip đứng đầu thuộc drive_a**. Phần lớn là trip chạy chậm: ở 2 khung/giây, xe gần như đứng yên thì các khung liên tiếp bị coi là gần trùng (khớp với 149 khung trùng của A so với 52 của B ở Câu 5). Truy ngược về silver (`report/step_outputs/metabase_custom_questions.txt`): ở cả 4 trip đứng đầu, **toàn bộ khung bị loại là do gần trùng**, không có khung đen, mờ hay mất đồng bộ (`trip-04622`: 34 trùng, `trip-02086`: 28, `trip-08888`: 24, `trip-03904`: 11). `trip-04622` chạy 67 km/h mà vẫn mất 34/60 khung. Em đoán đây là đoạn camera đứng hình 6 s, hoặc đoạn đường cao tốc có cảnh rất đồng đều làm dHash gần như không đổi. Như vậy, ngưỡng `dup_hamming` mới là "van" quyết định lượng dữ liệu của drive A. Ý nghĩa vận hành: `val` có 2 trip chậm của A nên chỉ giữ 71.0% khung, thấp hơn train (82.1%) và test (92.9%). Nếu ngưỡng trùng cố định, tập val sẽ thiếu cảnh tắc đường hay đi chậm.

**Câu hỏi 2 (vận hành, điểm cộng): tỉ lệ khung giữ lại theo split**

```sql
select split, count(*) as trips, sum(frames) as frames_in, sum(frames_kept) as frames_kept,
       round(100.0*sum(frames_kept)/sum(frames),1) as pct_kept, round(avg(avg_speed_kmh)::numeric,1) as avg_speed_kmh
from marts.mart_trip_summary group by 1 order by case split when 'train' then 1 when 'val' then 2 else 3 end
```

| split | trips | frames_in | frames_kept | pct_kept | avg_speed_kmh |
|---|---|---|---|---|---|
| train | 18 | 892 | 732 | 82.1 | 54.1 |
| val | 4 | 186 | 132 | 71.0 | 41.2 |
| test | 4 | 168 | 156 | 92.9 | 51.2 |

Bảng này giúp phát hiện khi một split bị lọc mạnh bất thường. Với MSSV của em, val nhận các trip chậm nên mất nhiều khung nhất. Chia theo trip tránh được rò rỉ nhưng không đảm bảo các split cân bằng về điều kiện lái, nên cần phân tầng thêm theo tốc độ hoặc ánh sáng.

## Nhìn lại

Điều bất ngờ nhất là chia ngẫu nhiên theo khung hình làm rò rỉ **toàn bộ 26/26 trip**, nên không có chuyện "chỉ rò một chút". Thứ hai là `dataset_version` đứng yên dù `silver.frames` đã lên Delta v27, vì version là băm nội dung chứ không theo số lần ghi. Điều em chưa rõ: vì sao verify chỉ bắt được 11/12 khung đứng hình ở A (có thể khung đầu tiên của đoạn đứng hình không có khung trước để so). Lỗi dữ liệu pipeline **chưa bắt**: telemetry "hợp lý nhưng sai", ví dụ cảm biến tốc độ bị kẹt một giá trị, hay GPS nhảy vài trăm mét. `in_range` vẫn PASS vì giá trị nằm trong 0..70 m/s. Nên thêm test so tốc độ với quãng đường GPS. Pipeline cũng chỉ trừ một **độ lệch đồng hồ cố định** đã khai báo; nếu đồng hồ trôi dần theo thời gian thì không phát hiện được. Về vận hành, mạng trong Docker chập chờn khiến em không build được image theo cách thường, phải tải wheel trên máy rồi cài offline. Điều này cho thấy việc ghim version và có artifact dựng sẵn quan trọng thế nào.
