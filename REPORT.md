# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Trần Đình Duy |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | 2A202602631 |
| GitHub | Duytd26 |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | trip-07900 |

## Kết quả cuối

- `dataset_version` của bạn: `<điền>`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `<điền>`
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

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
- Tốc độ tối đa sau chuẩn hóa hợp lý: Drive A đạt tối đa 71 km/h (19.82 m/s), Drive B đạt tối đa 100 km/h (27.83 m/s), đều dưới giới hạn vật lý 130 km/h.

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
- `max_gap_ms` (warn): Khoảng cách thời gian giữa hai bản ghi đo vượt quá 1000 ms chỉ phản ánh việc mất tín hiệu cảm biến tạm thời (sensor dropouts). Trong xe thực tế, hiện tượng này đôi khi xảy ra và là khiếm khuyết được dung thứ (tolerated defect). Pipeline gắn nhãn cảnh báo để kỹ sư kiểm tra nhưng không dừng hệ thống vì xe vẫn đang vận hành.
- `in_range` (error): Giá trị tốc độ xe vật lý bắt buộc phải nằm trong khoảng 0 đến 70 m/s (~252 km/h). Tốc độ âm hoặc tốc độ phi thực tế lọt vào bảng sạch là lỗi dữ liệu nghiêm trọng (fatal defect), sẽ làm sai lệch hoàn toàn thuật toán điều khiển và mô hình AI huấn luyện ở tầng sau, vì vậy phải cấu hình mức `error` để chặn đứng pipeline ngay tại chỗ.

2. Kết quả thí nghiệm `break-gate` (`break_gate.json`):
- Khi nới luật làm sạch từ 70 lên 1000 m/s: Các dòng có tốc độ phi lý không còn bị phân loại vào `rejects` mà lọt thẳng vào bảng `silver_telemetry`.
- Test `in_range` trên cột `speed_mps` lập tức bắt được chính xác **6 dòng** vi phạm (3 dòng ở drive A và 3 dòng ở drive B), làm dbt test chuyển trạng thái `FAIL` (Summary: `PASS=4 WARN=1 ERROR=1 SKIP=0`) và dừng pipeline.
- Khi khôi phục lại luật gốc: dbt test trở về trạng thái đạt `PASS=4 WARN=1 ERROR=0 SKIP=0`.
- **Vai trò của cổng chất lượng (Quality Gate)**: Hoạt động như chốt chặn độc lập bảo vệ các tầng hạ nguồn. Dù logic xử lý ở tầng trên có vô tình bị nới lỏng hay có lỗi tiềm ẩn lọt qua, cổng kiểm thử dữ liệu vẫn sẽ phát hiện và ngăn chặn không cho dữ liệu bẩn xâm nhập vào kho dữ liệu Gold và mô hình học máy.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

1. Kết quả từ `idempotency.json`:
- Khi chạy lại một partition (trip) 2 lần bằng cơ chế của pipeline (Delta Lake overwrite có predicate theo `trip_id`): Tổng số dòng trước và sau 2 lần chạy lại là hoàn toàn bằng nhau (`rows_before == rows_after_two_reruns`), và mã băm dấu vân tay (fingerprint) của toàn bảng giữ nguyên 100% không đổi (`fingerprint_before == fingerprint_after`, `unchanged: true`).
- Khi mô phỏng kiểu ghi `append` sai lầm: Sau 2 lần chạy lại, số dòng của trip đó bị nhân đôi (2x so với số lượng khung ban đầu của trip), dẫn đến việc dữ liệu bị trùng lặp nghiêm trọng.

2. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại:
- **Tính nguyên tử (ACID Atomicity)**: Thao tác `overwrite` có predicate trong Delta Lake diễn ra dưới dạng một transaction duy nhất. Dữ liệu mới được ghi vào các file parquet mới và chỉ được commit vào `_delta_log` khi toàn bộ quá trình thành công. Nếu giữa chừng xảy ra sự cố (node sập, ổ cứng đầy, mạng đứt), transaction bị hủy bỏ (rollback) và dữ liệu cũ của trip vẫn còn nguyên vẹn 100%, người dùng hay task khác không bao giờ nhìn thấy trạng thái bảng bị rỗng hoặc lỗi.
- **Tránh mất mát dữ liệu do ngắt quãng (Delete-then-Insert risk)**: Nếu dùng lệnh `DELETE` rồi sau đó mới `INSERT`, hệ thống sẽ rơi vào "khoảng chết" nguy hiểm. Nếu tiến trình bị sập sau bước xóa nhưng trước khi kịp chèn lại, dữ liệu của cả trip sẽ bị xóa vĩnh viễn (data loss).
- **Phạm vi cô lập (Isolation)**: Cơ chế `overwrite` có predicate chỉ thay thế đúng partition của trip được chỉ định, hoàn toàn không ảnh hưởng tới 25 trip còn lại đang được đọc hoặc ghi đồng thời.

### Câu 5
1. Phân biệt khung hình đen và khung hình tối:
- **Khung hình đen (`is_black`)**: Xảy ra do lỗi thiết bị (hỏng cảm biến camera, mất tín hiệu, bị che ống kính hoàn toàn), độ sáng trung bình $luma < 12.0$. Khung đen không chứa bất kỳ đặc trưng thị giác nào (không có cạnh, độ nét Laplacian variance $= 0.0$). Pipeline loại bỏ hoàn toàn khung đen khỏi tập huấn luyện Gold.
- **Khung hình tối (`is_dark`)**: Xảy ra khi xe đi qua đường hầm, bóng râm cây cối, gầm cầu hoặc trời chập tối ($12.0 \le luma < 40.0$). Khung hình vẫn hiển thị đầy đủ các chi tiết đường sá, làn đường, biển báo và xe khác (vẫn có độ nét và góc cạnh). Pipeline **gắn cờ nhưng giữ lại** (`toi_giu_lai`).
- **Đánh giá**: Tôi hoàn toàn đồng ý với cách xử lý này, vì xe tự lái trong thế giới thực phải có khả năng hoạt động tốt trong điều kiện thiếu sáng. Giữ lại khung tối giúp mô hình học cách quan sát ban đêm/trong hầm, trong khi loại khung đen giúp loại bỏ rác thuần túy.

2. Dựa vào `threshold.json` và phân tích dHash:

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 12 | 0 |
| 2 | 26 | 3 |
| 6 | 68 | 15 |

*(Lưu ý: Drive A có số lượng khung trùng cao hơn rõ rệt vì có đoạn camera đứng hình 6 giây liên tiếp và xe di chuyển ở dải tốc độ chậm trong đô thị).*
- **Lựa chọn ngưỡng**: Tôi chọn **ngưỡng 2 (mặc định của pipeline)**. Ngưỡng 0 quá chặt, dễ bỏ sót khung trùng do nhiễu cảm biến nhẹ (sensor noise 1-2 bit). Ngưỡng 6 quá lỏng, sẽ bắt nhầm và loại bỏ cả những khung hình có chuyển động thực tế hữu ích. Ngưỡng 2 cân bằng hoàn hảo giữa việc lọc khung đứng hình và giữ độ phong phú của dữ liệu.
- **Mối liên hệ với tốc độ xe**: Tốc độ xe càng cao thì bối cảnh giữa các khung hình (cách nhau 0.5s) thay đổi càng nhanh, khoảng cách Hamming giữa các mã dHash càng lớn ($> 8$ bit). Ngược lại, khi xe dừng đèn đỏ hoặc di chuyển chậm, khung hình hầu như không đổi nên khoảng cách dHash rất nhỏ ($\le 2$ bit).

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

1. Dựa vào `leakage.json`:
- **Chia theo khung hình (Random 70/15/15)**: Toàn bộ **26/26 trip** bị chia cắt (xuất hiện ở nhiều hơn 1 split), và **26/26 trip** có khung hình xuất hiện đồng thời ở cả tập `train` và tập `test`.
- **Chia theo trip (Gold của pipeline)**: Có đúng **0 trip** bị chia cắt (`per_trip_split_trips_straddling = 0`). Mỗi trip nằm trọn vẹn trong duy nhất 1 split (`train`, `val` hoặc `test`).

2. Vì sao chia theo khung hình làm điểm kiểm thử đẹp một cách giả tạo:
- Các khung hình liên tiếp trong cùng một chuyến đi (cách nhau 0.5s ở 2 fps) có độ tương quan thị giác cực kỳ cao (gần như giống hệt nhau về góc camera, khung cảnh, thời tiết, điều kiện đường sá).
- Nếu chia theo khung hình, mô hình học khung hình ở giây $t$ (trong `train`) rồi dự đoán khung hình ở giây $t+0.5$ (trong `test`). Khi đó mô hình thực chất chỉ "học vẹt" (ghi nhớ đề thi) thay vì học cách tổng quát hóa (generalization) trên các cung đường mới lạ. Điểm kiểm thử trên tập test sẽ cao chót vót nhưng khi đem mô hình ra lái trên thực tế sẽ thất bại ngay lập tức.

### Câu 7
1. Vì sao `dataset_version` là **băm nội dung** (content hash) chứ không phải ngày giờ hay số đếm:
- `dataset_version` được tạo bằng mã băm SHA256 của toàn bộ danh sách các cặp `frame_id|split` được sắp xếp theo thứ tự xác định (`hashlib.sha256("\n".join(f"{i}|{s}" for ...)).hexdigest()[:12]`).
- Băm nội dung đảm bảo tính xác định và tái lập (deterministic & reproducible): Bất kể bạn chạy lại pipeline bao nhiêu lần, ở ngày giờ nào hay trên máy tính nào, miễn là dữ liệu đầu vào và cách chia split giống nhau thì mã phiên bản sinh ra là hoàn toàn như nhau. Nó đại diện chính xác cho nội dung dữ liệu thay vì thời điểm chạy máy.

2. Hai lần chạy liên tiếp cho kết quả gì:
- Cho ra cùng một chuỗi `dataset_version` giống hệt nhau 100%, không bị tăng số đếm hay sinh ra phiên bản rác.

3. Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu gốc:
- Trong mô hình `gold_dataset_frames.sql`, pipeline sắp xếp các trip để chia tỷ lệ 70/15/15 dựa trên hàm băm có salt là MSSV: `order by md5(trip_id || '{{ env_var("LAB_STUDENT_ID", "") }}')`.
- Vì MSSV của tôi (`2A202602631`) là duy nhất, thứ tự phân bổ các trip vào các split `train`, `val`, `test` của tôi sẽ khác với các bạn khác, dẫn đến tổ hợp các cặp `(frame_id, split)` khác nhau, tạo ra `dataset_version` riêng biệt đại diện cho bài làm cá nhân của tôi.

### Câu 8
1. Đọc phần `lineage` trong `manifest.json`:
Dataset này được tạo trực tiếp từ:
- Hai file video gốc có mã SHA256:
  + `drive_a`: `636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa`
  + `drive_b`: `bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc`
- Các bảng Bronze: `raw_telemetry_a` (version 0), `raw_telemetry_b` (version 0), `trips_raw` (version 0), `video_files` (version 0).
- Các bảng Silver: `frames` (version 0), `telemetry` (version 0), `frames_flagged` (version 0), `frame_telemetry` (version 0).
- Bộ luật loại trừ: `is_black`, `is_blurry`, `is_near_dup`, `not sync_ok`.

2. Thông tin giúp tái tạo lại đúng dataset này sau 3 tháng:
- Nhờ Delta Lake có tính năng **Time Travel**, chỉ cần 3 thông tin ghi trong manifest:
  + File video gốc (kiểm tra bằng mã SHA256 tương ứng).
  + Số phiên bản commit cụ thể của các bảng Delta ở Bronze và Silver (ví dụ version 0).
  + Mã MSSV (`student_id = 2A202602631`) để khôi phục đúng seed phân chia split. Khi đó ta có thể tái tạo 100% chính xác từng khung hình của dataset này.

## Bước 5 · Điều phối và phục vụ

### Câu 9
1. Điều xảy ra khi trip của bạn (`trip-07900`) sập giả lập (`LAB_FAIL_ONCE=trip-07900`):
- Đúng **1 task** trích xuất của trip đó (`extract_trip_trip_07900`) chuyển sang màu **ĐỎ (FAILED)** với log thông báo lỗi: `RuntimeError: simulated crash while extracting trip-07900`.
- 25 task trích xuất của các trip khác vẫn chạy độc lập và chuyển **XANH**.
- Toàn bộ các task phía sau (`dbt_run_silver`, `dbt_test_silver`, `publish_lake`, `gold_dataset`, `publish_warehouse`) chuyển sang trạng thái **`upstream_failed`** màu cam/xám, bị chặn lại không cho chạy tiếp.

2. Thao tác xử lý và kết quả:
- Trên giao diện Airflow Graph, bấm vào duy nhất task đỏ `extract_trip_trip_07900`, chọn **Clear** (chỉ task đó). Task này chạy lại lần 2 và chuyển sang màu **XANH**.
- Các task `upstream_failed` tự động được kích hoạt và chạy tiếp cho tới khi toàn bộ DAG chuyển xanh hoàn toàn.
- **Dữ liệu KHÔNG bị nhân đôi**: Nhờ cơ chế Delta Lake `mode="overwrite", predicate="trip_id = 'trip-07900'"`, partition cũ của trip được ghi đè nguyên tử.
- **`dataset_version` KHÔNG đổi**: Sau khi chạy lại, tập dữ liệu đầu ra và băm nội dung vẫn giữ nguyên giá trị ban đầu.
- **Bằng chứng**: Ảnh chụp màn hình log task đỏ hiển thị đúng `simulated crash while extracting trip-07900`, ảnh DAG xanh lại sau khi clear, và kết quả lệnh `scripts/lab.sh verify` vẫn đạt 27/27.

### Câu 10
Khi nâng quy mô từ 2 drive lên **500 xe mỗi ngày** (~2.500 giờ video và hàng trăm triệu dòng telemetry):
1. **Thành phần giải mã video tuần tự bằng FFmpeg trên 1 container**:
   - *Vấn đề*: Decode video là nút thắt cổ chai tính toán nặng nhất (slide 24). Một container đơn lẻ sẽ cạn kiệt CPU, RAM và mất nhiều ngày để xử lý 500 xe.
   - *Thay thế bằng*: Kiến trúc phân tán (Distributed Processing) dùng **Apache Spark** trên cụm Kubernetes hoặc dịch vụ serverless như AWS Batch, tận dụng GPU để giải mã video song song theo batch.
2. **Công cụ lưu trữ & truy vấn DuckDB đơn máy (`lakehouse.duckdb`)**:
   - *Vấn đề*: DuckDB chạy in-process trên tài nguyên một máy cục bộ. Với dữ liệu hàng terabyte, dung lượng đĩa và RAM sẽ bị tràn (OOM).
   - *Thay thế bằng*: Công cụ truy vấn Lakehouse phân tán như **Apache Spark (Databricks)** hoặc **Trino / Presto** truy vấn trực tiếp trên Delta Lake / Iceberg lưu trên S3 (slide 22).
3. **Airflow LocalExecutor và PostgreSQL Warehouse đơn lẻ**:
   - *Vấn đề*: LocalExecutor chỉ điều phối trên 1 máy, không scale được hàng chục nghìn task; PostgreSQL truyền thống không chịu nổi hàng trăm triệu bản ghi phân tích telemetry (OLAP).
   - *Thay thế bằng*: Nâng cấp Airflow sang **KubernetesExecutor / CeleryExecutor**; chuyển Warehouse phân tích sang **ClickHouse** hoặc **Snowflake / Google BigQuery** tối ưu hóa cho truy vấn phân tích cột (OLAP) quy mô lớn (slide 16).

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:
```sql
select t.lighting,
       count(*) as tong_so_khung,
       count(*) filter (where is_black or is_blurry or is_near_dup) as so_khung_bi_loai,
       round(count(*) filter (where is_black or is_blurry or is_near_dup) * 100.0 / count(*), 2) as ty_le_loai_pct
from marts.mart_frame_gallery g
join marts.mart_trip_summary t using (trip_id)
group by 1
order by 1;
```
- **Ý nghĩa**: Câu truy vấn phân tích tỷ lệ khung hình bị loại bỏ (do đen, mờ hoặc trùng) theo từng điều kiện ánh sáng (`lighting`: Day, Night, Clouds...). Nó giúp kỹ sư đánh giá chất lượng quang học của camera xem hệ thống cảm biến hoạt động kém nhất trong điều kiện ánh sáng nào để có giải pháp phần cứng phù hợp.

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

- **Bất ngờ nhất**: Cơ chế ghép nối ASOF join theo Event Time trong DuckDB hoạt động cực kỳ hiệu quả, tự động đồng bộ khung hình với bản ghi telemetry gần nhất dù hai cảm biến có tần số lấy mẫu và độ trễ khác nhau.
- **Điều còn cần đào sâu**: Cách thiết lập ngưỡng phân mảnh tối ưu khi kích thước partition Delta quá nhỏ trong môi trường hàng trăm camera stream đồng thời.
- **Lỗi dữ liệu pipeline chưa bắt được**: Hiện tượng chói nắng (glare / overexposure) khi xe chạy ngược hướng mặt trời gắt làm trắng xóa phần lớn biển báo, nhưng độ sáng tổng thể vẫn cao và độ nét biên không quá thấp nên không bị bắt bởi `is_black` hay `is_blurry`.
