# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Trần Đình Duy |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | 2A202602631 |
| GitHub | Duytd26 |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | trip-07900 |

## Kết quả cuối

- `dataset_version` của bạn: `c8936824962d`  (khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: **705 / 157 / 157** (tổng 1.019; 18 / 4 / 4 trip)
- Lượt chạy cuối từ lake trống ngày **13/10/2026, 09:03 (UTC+7)** đạt toàn bộ năm bước và 27/27. Export lại giữ nguyên phiên bản, số khung và số trip: `report/evidence/export_stability.json` ghi `unchanged=true`.
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, `airflow_crash_task.png` và `airflow_retry_graph.png` (run crash/retry đã phục hồi).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

1. Bốn khác biệt lớn giữa telemetry nguồn A và nguồn B ở Bronze:
- **Thời gian (Timestamp)**: Nguồn A sử dụng định dạng ISO 8601 UTC string (`ts_utc`, ví dụ `2025-10-07T08:00:00.014Z`), trong khi Nguồn B sử dụng epoch timestamp dạng số mili-giây (`timestamp_ms`, ví dụ `1759933801509`).
- **Tốc độ và Đơn vị**: Nguồn A lưu cột `speed_mps` với đơn vị mét/giây (m/s); Nguồn B lưu cột `spd_kmh` với đơn vị kilômét/giờ (km/h).
- **Đặt tên cột (Schema drift)**: Nguồn A dùng tên cột viết tắt/chi tiết như `lat`, `lon`, `alt_m`, `heading_deg`, `acc_x`, `acc_y`, `steer`; còn Nguồn B dùng `latitude`, `longitude`, `altitude`, `hdg`, `ax`, `ay`, `steering`.
- **Thang đo chân ga / phanh**: Nguồn A biểu diễn tỉ lệ thập phân từ 0.0 đến 1.0 (`gas`, `brake`), còn Nguồn B biểu diễn theo tỉ lệ phần trăm từ 0 đến 100% (`throttle_pct`, `brake_pct`).
- **Độ lệch đồng hồ (Clock Offset)**: Theo `sources.json`, telemetry A khớp đồng hồ với video (`offset = 0 ms`), trong khi telemetry B bị lệch đồng hồ 1500 ms (`offset = 1500 ms`) so với video.

2. Lý do pipeline giữ mỗi nguồn một bảng Bronze riêng và lưu mọi cột dạng chuỗi (text/string):
- **Nguyên tắc raw landing (bất biến, trung thực)**: Tầng Bronze là tầng ghi nguyên bản không can thiệp, phản ánh chính xác dữ liệu từ cảm biến gửi về. Do hai nguồn có cấu trúc khác nhau (schema drift), việc giữ từng bảng riêng biệt giúp tránh làm vỡ schema hoặc tạo ra bảng hỗn hợp nhiều giá trị null.
- **Không ép kiểu ngay để tránh crash pipeline**: Nếu ép kiểu float, int hay datetime ngay tại Bronze, bất kỳ dòng dữ liệu lỗi nào (ô trống, chuỗi bẩn, định dạng lạ) sẽ khiến ingestion sập ngay lập tức hoặc làm mất dữ liệu. Việc giữ nguyên chuỗi cho phép nhận 100% dữ liệu vào Lake an toàn, lưu lại vết (lineage/audit), và chuyển trách nhiệm làm sạch/ép kiểu/bắt lỗi sang tầng Silver.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

1. Cách pipeline đưa hai nguồn về một schema:
- **Chuẩn hóa tốc độ**: Telemetry A đã ở đơn vị m/s nên giữ nguyên. Telemetry B ở đơn vị km/h được quy đổi sang m/s trong `stg_telemetry_b.sql` bằng công thức: `try_cast(nullif(t.spd_kmh, '') as double) / 3.6 as speed_mps`.
- **Chuẩn hóa thời gian và bù lệch đồng hồ (Clock Offset)**: Telemetry A parse trực tiếp từ chuỗi ISO UTC `ts_utc` sang `timestamptz`. Telemetry B được đọc từ epoch milliseconds (`timestamp_ms`), trừ đi độ lệch đồng hồ 1500 ms khai báo trong `sources.json` (lưu tại `bronze.video_files`), rồi đổi sang giây: `to_timestamp((cast(t.timestamp_ms as bigint) - off.offset_ms) / 1000.0) as ts`.
- **Chuẩn hóa chân ga, phanh và cột khác**: Đổi tên các cột của B (`latitude -> lat`, `longitude -> lon`, `hdg -> heading_deg`, `ax/ay -> acc_x/acc_y`, `steering -> steer`), đưa thang đo chân ga/phanh từ phần trăm (%) về thang 0.0 - 1.0 (`throttle_pct / 100 as gas`, `brake_pct / 100 as brake`).

2. Bằng chứng kết quả đã đúng:
- Mốc bắt đầu telemetry B sau khi trừ offset là `2025-10-08 14:30:00.009+00:00`, khớp chính xác với mốc bắt đầu của video `drive_b.mp4` (`2025-10-08 14:30:00`) trong khoảng sai lệch chưa đầy 10 ms (< 0.5 s).
- Tốc độ tối đa sau chuẩn hóa hợp lý: Drive A đạt tối đa 71 km/h (19.82 m/s), Drive B đạt tối đa 100 km/h (27.83 m/s), đều dưới mốc 130 km/h dùng để quan sát trong lab; luật gate thực tế là 0–70 m/s.

3. Bảng quarantine (số dòng theo lý do, từng drive):

| drive_id | reject_reason | so_dong |
|---|---|---|
| drive_a | duplicate | 20 |
| drive_a | speed_missing | 30 |
| drive_a | speed_out_of_range | 3 |
| drive_b | duplicate | 20 |
| drive_b | speed_missing | 30 |
| drive_b | speed_out_of_range | 3 |

*Tổng cộng mỗi drive có 53 dòng lỗi bị cách ly. Số dòng sạch thu được: Drive A có 2,968 dòng; Drive B có 3,181 dòng. Tổng dòng sạch + quarantine: Drive A = 2,968 + 53 = 3,021; Drive B = 3,181 + 53 = 3,234 (khớp 100% dữ liệu gốc).*

4. Vì sao dòng lỗi vào `rejects` thay vì bị xóa:
- **Truy vết và giám sát (Audit & Observability)**: Giữ lại dòng lỗi kèm `reject_reason` giúp kỹ sư theo dõi tỷ lệ lỗi, phát hiện cảm biến nào đang có vấn đề bất thường mà không làm mất thông tin.
- **Khả năng tái xử lý (Reprocessing)**: Khi có thuật toán làm sạch tốt hơn (ví dụ nội suy các dòng thiếu tốc độ) hoặc hiệu chỉnh lại luật lọc, dữ liệu trong `rejects` vẫn còn nguyên để phục hồi lại thay vì bị xóa im lặng (silent data loss).

### Câu 3
1. Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`:
- `max_gap_ms` (warn): `schema.yml` cấu hình rõ `severity: warn` cho khoảng trống telemetry vượt 1.000 ms. Lab cho pipeline tiếp tục nhưng cần điều tra khoảng trống; khung mất đồng bộ được gắn cờ và loại khỏi Gold. Khoảng trống không tự xác định nguyên nhân, và lựa chọn warn không chứng minh hệ thống an toàn để vận hành xe thực tế.
- `in_range` (error): Test kiểm tra luật tốc độ **0–70 m/s** của lab; không ghi đè severity nên dùng mức error mặc định. Đây là giới hạn chất lượng cấu hình, không phải giới hạn vật lý chung cho mọi xe. Nếu dòng vi phạm lọt vào Silver, dbt trả lỗi để chặn bước hạ nguồn.

2. Kết quả thí nghiệm `break-gate` (`break_gate.json`):
- Khi nới luật làm sạch từ 70 lên 1000 m/s: Các dòng có tốc độ phi lý không còn bị phân loại vào `rejects` mà lọt thẳng vào bảng `silver_telemetry`.
- Test `in_range` trên cột `speed_mps` lập tức bắt được chính xác **6 dòng** vi phạm (3 dòng ở drive A và 3 dòng ở drive B), làm dbt test chuyển trạng thái `FAIL` (Summary: `PASS=5 WARN=1 ERROR=1 SKIP=0`) và dừng pipeline.
- Khi khôi phục lại luật gốc: dbt test trở về trạng thái đạt `PASS=6 WARN=1 ERROR=0 SKIP=0`.
- **Vai trò của cổng chất lượng (Quality Gate)**: Hoạt động như chốt chặn độc lập bảo vệ các tầng hạ nguồn. Dù logic xử lý ở tầng trên có vô tình bị nới lỏng hay có lỗi tiềm ẩn lọt qua, cổng kiểm thử dữ liệu vẫn sẽ phát hiện và ngăn chặn không cho dữ liệu bẩn xâm nhập vào kho dữ liệu Gold và mô hình học máy.

## Bước 3 · Silver frames

### Câu 4

Theo `idempotency.json`, chạy lại **trip-07900** hai lần giữ nguyên **1.246 dòng**, fingerprint trước/sau đều **`6c7480dfde7e`**, `unchanged=true`. Trip có **30 khung**; append hai lần tạo **60 dòng** (2x).

Delta overwrite với predicate trip_id thay dữ liệu của một trip bằng một transaction. Lỗi trước commit không làm mất snapshot cũ; độc giả chỉ thấy snapshot đã commit. Xóa rồi ghi thành hai bước có thể để lại khoảng trống nếu tiến trình sập giữa hai bước. Tính nguyên tử này áp dụng cho bảng Delta; ảnh JPG trên object storage không nằm trong cùng một transaction với bảng.

### Câu 5

Thống kê từ lượt chạy cuối, đối chiếu `run_summary.json`:

| Drive | Tổng frame | Đen | Mờ | Gần trùng | Tối (gắn cờ) | Mất đồng bộ |
|---|---:|---:|---:|---:|---:|---:|
| drive_a | 602 | 6 | 9 | 149 | 4 | 7 |
| drive_b | 644 | 4 | 0 | 53 | 61 | 7 |

Một frame có thể mang nhiều cờ. Số tối là số được gắn `is_dark`, không đồng nghĩa tất cả đã vào Gold: frame tối vẫn bị loại nếu đồng thời mờ, gần trùng hoặc mất đồng bộ. Độ trễ đồng bộ trung bình A/B là **66/73 ms**; tối đa **2.884/3.112 ms**, còn ngưỡng giữ của lab là **150 ms**.

Đã xem trực tiếp ba ảnh mẫu từ MinIO và đối chiếu metadata (`silver_frame_samples.json`): `drive_a-01001` đen hoàn toàn, brightness **0**; `drive_a-01998` mờ rõ, blur_score **2,485**; `drive_a-00545` còn thấy đường/cây dù cảnh tối, brightness **39,647**, không mờ/trùng và sync gap **91 ms**, nên đủ điều kiện giữ. Mẫu này minh họa các cờ, không đại diện cho toàn bộ ảnh trong dataset. Unit tests kiểm tra dHash/độ nét đạt **9/9** ở lần kiểm tra cuối.

1. Phân biệt khung hình đen và khung hình tối:
- **Khung hình đen (`is_black`)**: Lab dùng ngưỡng độ sáng trung bình $luma < 12.0$ và loại khỏi Gold. Ngưỡng này không tự chứng minh ảnh hoàn toàn mất chi tiết hoặc xác định nguyên nhân hỏng camera; cần xem ảnh để đánh giá.
- **Khung hình tối (`is_dark`)**: Lab gắn cờ khi $12.0 \le luma < 40.0$. Khung được giữ nếu không vi phạm luật khác (mờ, trùng, mất đồng bộ), để duy trì dữ liệu thiếu sáng có thể vẫn hữu ích.
- **Đánh giá**: Cách phân biệt phù hợp với mục đích của lab, nhưng các ngưỡng là heuristic cần kiểm tra trên ảnh mẫu, không bảo đảm mọi khung giữ lại đều hữu ích.

2. Dựa vào `threshold.json` và phân tích dHash:

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

*(Lưu ý: Drive A có số lượng khung trùng cao hơn rõ rệt vì có đoạn camera đứng hình 6 giây liên tiếp và xe di chuyển ở dải tốc độ chậm trong đô thị).*
- **Lựa chọn ngưỡng**: Tôi giữ **ngưỡng 2 (mặc định của pipeline)**. Ngưỡng 0 có thể bỏ sót ảnh gần trùng do nhiễu nhẹ; ngưỡng 6 loại nhiều hơn và có thể loại cả khung có thay đổi hữu ích. Chưa có nhãn để chứng minh ngưỡng 2 tối ưu; thí nghiệm chỉ đo độ nhạy số lượng theo ngưỡng.
- **Mối liên hệ với tốc độ xe**: Tốc độ xe càng cao thì bối cảnh giữa các khung hình (cách nhau 0.5s) thay đổi càng nhanh, khoảng cách Hamming giữa các mã dHash càng lớn (không có ngưỡng bit cố định chỉ từ tốc độ). Ngược lại, khi xe dừng đèn đỏ hoặc di chuyển chậm, khung hình hầu như không đổi nên khoảng cách dHash rất nhỏ (có thể nhỏ, nhưng còn phụ thuộc cảnh, vật thể và nhiễu).

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

1. Dựa vào `leakage.json`:
- **Chia theo khung hình (Random 70/15/15)**: Toàn bộ **26/26 trip** bị chia cắt (xuất hiện ở nhiều hơn 1 split), và **26/26 trip** có khung hình xuất hiện đồng thời ở cả tập `train` và tập `test`.
- **Chia theo trip (Gold của pipeline)**: Có đúng **0 trip** bị chia cắt (`per_trip_split_trips_straddling = 0`). Mỗi trip nằm trọn vẹn trong duy nhất 1 split (`train`, `val` hoặc `test`).

2. Vì sao chia theo khung hình làm điểm kiểm thử đẹp một cách giả tạo:
- Các khung hình liên tiếp trong cùng một chuyến đi (cách nhau 0.5s ở 2 fps) có độ tương quan thị giác cực kỳ cao (gần như giống hệt nhau về góc camera, khung cảnh, thời tiết, điều kiện đường sá).
- Nếu chia theo khung hình, mô hình học khung hình ở giây $t$ (trong `train`) rồi dự đoán khung hình ở giây $t+0.5$ (trong `test`). Khi đó mô hình thực chất chỉ "học vẹt" (ghi nhớ đề thi) thay vì học cách tổng quát hóa (generalization) trên các cung đường mới lạ. Điểm test có thể lạc quan hơn khả năng tổng quát hóa. Thí nghiệm này chỉ đo rò rỉ theo trip, chưa huấn luyện mô hình nên không đo mức tăng điểm hay kết luận hiệu năng lái thực tế.

### Câu 7
1. Vì sao `dataset_version` là **băm nội dung** (content hash) chứ không phải ngày giờ hay số đếm:
- `dataset_version` được tạo bằng mã băm SHA256 của toàn bộ danh sách các cặp `frame_id|split` được sắp xếp theo thứ tự xác định (`hashlib.sha256("\n".join(f"{i}|{s}" for ...)).hexdigest()[:12]`).
- Băm nội dung đảm bảo tính xác định và tái lập (deterministic & reproducible): Bất kể bạn chạy lại pipeline bao nhiêu lần, ở ngày giờ nào hay trên máy tính nào, miễn là dữ liệu đầu vào và cách chia split giống nhau thì mã phiên bản sinh ra là hoàn toàn như nhau. Nó đại diện chính xác cho nội dung dữ liệu thay vì thời điểm chạy máy.

2. Kết quả chạy và đối chiếu sau phục hồi:
- Phiên bản trước và sau DAG crash/retry đều là `c8936824962d`; số train/val/test vẫn 705/157/157. DAG phục hồi thành công với 37/37 task success, task silver_frames[21] chạy 2 lần; verify sau phục hồi đạt 27/27. Hàm băm dùng danh sách `frame_id|split`, không băm mọi byte ảnh hoặc mọi giá trị telemetry.

3. Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu gốc:
- Trong mô hình `gold_dataset_frames.sql`, pipeline sắp xếp các trip để chia tỷ lệ 70/15/15 dựa trên hàm băm có salt là MSSV: `order by md5(trip_id || '{{ env_var("LAB_STUDENT_ID", "") }}')`.
- MSSV (`2A202602631`) ảnh hưởng thứ tự phân bổ trip và thường tạo split khác. Không bảo đảm mọi cặp MSSV có phiên bản khác nhau: có thể cùng phân bổ trip, và băm 12 ký tự cũng không phải chứng minh duy nhất tuyệt đối.

### Câu 8

Manifest thực tế của lượt chạy cuối được lưu tại `report/evidence/dataset_manifest.json`, lấy từ `lake/gold/datasets/c8936824962d/manifest.json`. Bản sau DAG phục hồi trước đó được giữ riêng tại `dataset_manifest_crash_retry.json`.

| Bảng upstream trong lineage | Version ở lượt chạy cuối |
|---|---:|
| bronze.raw_telemetry_a | 0 |
| bronze.raw_telemetry_b | 0 |
| bronze.trips_raw | 0 |
| bronze.video_files | 1 |
| silver.frames | 25 |
| silver.telemetry | 0 |
| silver.frames_flagged | 0 |
| silver.frame_telemetry | 0 |

SHA256 video A: `636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa`; video B: `bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc`. Gold có train/val/test và manifest.json; danh sách và số ảnh từng thư mục được kiểm tra qua API MinIO trong `gold_dataset_contents.json`.

Để tái tạo sau ba tháng phải giữ video khớp SHA256, snapshot Delta cùng file dữ liệu chưa bị vacuum/xóa, code và cấu hình ngưỡng, môi trường giải mã, và MSSV 2A202602631 để chia split. Lineage giúp truy vết; chỉ có số version khi các file snapshot đã mất thì không đủ để time travel.

## Bước 5 · Điều phối và phục vụ

### Câu 9

Bật LAB_FAIL_ONCE=trip-07900, chạy reset rồi DAG run **lab-crash-retry-20261013**. Task thật là **silver_frames, map_index=21**. Kết quả trước phục hồi: **1 failed, 29 success, 7 upstream_failed**. Log ghi `RuntimeError: simulated crash while extracting trip-07900; run it again`. 25 instance trích xuất khác vẫn thành công.

Lưu ảnh log lỗi, rồi clear instance lỗi cùng downstream cần chạy lại; không chạy lại các instance trích xuất đã thành công. Marker `.failed-trip-07900` ngăn lỗi giả lập lần hai. Khi downstream đã bị đánh dấu upstream_failed, có thể cần clear chúng; không mặc định clear một task sẽ tự phục hồi mọi task sau. Sau phục hồi, kiểm tra DAG xanh, dataset_version giữ `c8936824962d`, verify 27/27, rồi xóa LAB_FAIL_ONCE khỏi .env. Bằng chứng trạng thái được ghi trong `report/evidence/crash_retry.json` và ảnh chụp thật.

### Câu 10

Ở quy mô 500 xe/ngày cần đo CPU, I/O, thời gian xử lý và kích thước dữ liệu trước khi chọn kiến trúc. Container FFmpeg và task trích xuất tuần tự là giới hạn hiện tại; có thể phân phối theo trip trên nhiều worker/batch cluster, nhưng phải kiểm soát xung đột ghi Delta. GPU decode cần benchmark, không mặc định luôn nhanh hơn toàn pipeline.

DuckDB và local work disk phụ thuộc tài nguyên một máy. Có thể dùng Spark/Trino khi workload cần phân tán; dữ liệu lớn không tự động đồng nghĩa DuckDB sẽ OOM. Airflow LocalExecutor có thể chuyển sang worker phân tán; warehouse có thể chuyển sang hệ cột khi truy vấn và lưu lượng vượt năng lực Postgres. Chọn dựa trên số đo và chi phí vận hành, không chỉ tên công nghệ.

## Câu hỏi riêng của bạn trên Metabase

**Có bao nhiêu khung bị loại khỏi Gold theo điều kiện ánh sáng?**

```sql
select lighting,
       sum(frames) as tong_so_khung,
       sum(frames - frames_kept) as so_khung_bi_loai,
       round(sum(frames - frames_kept) * 100.0 / nullif(sum(frames), 0), 2) as ty_le_loai_pct
from marts.mart_trip_summary
group by lighting
order by lighting;
```

Kết quả: **Day — 1.246 khung, 227 bị loại, 18,22%**. Bộ dữ liệu chỉ có lighting Day, chưa thể so sánh ngày/đêm. Số bị loại gồm mọi luật Gold, kể cả mất đồng bộ, và không đếm trùng các cờ.

Query cũ không chạy được vì gallery chỉ chứa khung đã giữ và không có is_black/is_blurry/is_near_dup. Query sửa dùng số tổng/giữ ở cấp trip. Đã thêm câu hỏi vào http://localhost:3000/dashboard/3; câu hỏi http://localhost:3000/question/47. Script tái tạo: `scripts/add_metabase_question.py`.

## Bonus · Câu hỏi Metabase thứ hai

**Trip nào có tỷ lệ khung bị loại cao nhất để ưu tiên kiểm tra chất lượng?**

```sql
select trip_id, drive_id, frames, frames_kept,
       frames - frames_kept as frames_dropped,
       round((frames - frames_kept) * 100.0 / nullif(frames, 0), 2) as dropped_pct,
       lighting, conditions
from marts.mart_trip_summary
order by dropped_pct desc, trip_id
limit 5;
```

| Trip | Tổng khung | Giữ | Loại | Tỷ lệ loại (%) |
|---|---:|---:|---:|---:|
| trip-04622 | 60 | 26 | 34 | 56,67 |
| trip-03904 | 22 | 11 | 11 | 50,00 |
| trip-02086 | 60 | 32 | 28 | 46,67 |
| trip-08888 | 60 | 36 | 24 | 40,00 |
| trip-07774 | 34 | 21 | 13 | 38,24 |

Cả năm trip thuộc drive A, lighting Day, conditions Clouds. Xếp hạng giúp ưu tiên kiểm tra trip-04622, nhưng các nhãn này không chứng minh thời tiết là nguyên nhân. Kết quả được lưu trong `report/evidence/bonus_metabase.json`; truy vấn điều tra cờ từng trip nằm trong `bonus_trip_reasons.txt`. Không cộng các cờ vì một khung có thể mang nhiều lỗi. Câu hỏi thứ hai đã thêm vào dashboard riêng: http://localhost:3000/dashboard/3, câu hỏi http://localhost:3000/question/48.

Điều tra Silver cho thấy ở năm trip này, số gần trùng lần lượt **34/11/28/24/13**, đúng bằng số bị loại; các cờ đen, mờ và mất đồng bộ đều bằng **0**. Như vậy nguyên nhân loại theo luật pipeline ở nhóm top 5 là `is_near_dup`. Điều này chưa xác định nguyên nhân vật lý (xe dừng, cảnh ít đổi hay camera đứng hình); cần xem ảnh/video trước khi kết luận thiết bị lỗi.

Bonus này dùng lựa chọn câu hỏi Metabase thứ hai theo RUBRIC.md. Không thay cấu hình split hay sync_tol_ms; MSSV và ngưỡng của bài nộp chính được giữ nguyên. Điểm cộng thực tế do giảng viên chấm, không được đảm bảo tự động.

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

- **Bất ngờ nhất**: Cơ chế ghép nối ASOF join theo Event Time trong DuckDB hoạt động cực kỳ hiệu quả, tự động đồng bộ khung hình với bản ghi telemetry gần nhất dù hai cảm biến có tần số lấy mẫu và độ trễ khác nhau.
- **Điều còn cần đào sâu**: Cách thiết lập ngưỡng phân mảnh tối ưu khi kích thước partition Delta quá nhỏ trong môi trường hàng trăm camera stream đồng thời.
- **Lỗi dữ liệu pipeline chưa bắt được**: Hiện tượng chói nắng (glare / overexposure) khi xe chạy ngược hướng mặt trời gắt làm trắng xóa phần lớn biển báo, nhưng độ sáng tổng thể vẫn cao và độ nét biên không quá thấp nên không bị bắt bởi `is_black` hay `is_blurry`.

Ghi chú vận hành Windows: .env đã chuyển UTF-16 LE sang UTF-8; entrypoint đã chuyển CRLF sang LF. .gitattributes giữ LF khi checkout; các file được bảo vệ có hash khớp bản gốc. Dữ liệu đầu vào đạt 6/6 checksum, unit tests đạt 9/9.
