# Báo Cáo Cập Nhật Dự Án — Tiến Trình Nâng Cấp Portfolio

> **Căn cứ nâng cấp:** Dựa theo khuyến nghị từ [HuongDan_NangCap_Portfolio.md](file:///D:/Big_Data/Fraud_Detetion_Flink/HuongDan_NangCap_Portfolio.md)  
> **Trạng thái hiện tại:** Đã hoàn thành toàn bộ **Giai đoạn 1 đến 6**:
> - Giai đoạn 1 & 2: Tái cấu trúc Package, Tự chủ POJO, Sửa Blocking I/O.
> - Giai đoạn 3: Hiện thực hóa Event Time & Watermark (Out-of-order 5s).
> - Giai đoạn 4: Quản trị bộ nhớ State TTL chống OOM.
> - Giai đoạn 5: Stateful Testing với Flink TestHarness.
> - Giai đoạn 6: Tích hợp Apache Kafka Sink & Kiến trúc Dual-Sink phân tán.

---

## MỤC LỤC
1. [Giai đoạn 1 & 2: Tái Cấu Trúc Package, POJO & Non-blocking Sink](#1-giai-đoạn-1--2-tái-cấu-trúc-package-pojo--non-blocking-sink)
2. [Giai đoạn 3: Khai Phóng Sức Mạnh Event Time & Watermark](#2-giai-đoạn-3-khai-phóng-sức-mạnh-event-time--watermark)
3. [Giai đoạn 4: Quản Trị Bộ Nhớ State TTL & Phòng Chống OOM](#3-giai-đoạn-4-quản-trị-bộ-nhớ-state-ttl--phòng-chống-oom)
4. [Giai đoạn 5: Kiểm Thử Trạng Thái (Stateful Unit Tests với Flink TestHarness)](#4-giai-đoạn-5-kiểm-thử-trạng-thái-stateful-unit-tests-với-flink-testharness)
5. [Giai đoạn 6: Tích Hợp Apache Kafka Sink & Kiến Trúc Dual-Sink](#5-giai-đoạn-6-tích-hợp-apache-kafka-sink--kiến-trúc-dual-sink)
6. [Cấu Trúc Source Code Hiện Tại](#6-cấu-trúc-source-code-hiện-tại)
7. [Bảng Tổng Hợp Kỹ Thuật Theo Chuẩn Doanh Nghiệp](#7-bảng-tổng-hợp-kỹ-thuật-theo-chuẩn-doanh-nghiệp)
8. [Kết Quả Biên Dịch & Chạy Test Tự Động (Verification)](#8-kết-quả-biên-dịch--chạy-test-tự-động-verification)
9. [Hướng Dẫn Chạy Toàn Diện Hệ Thống (End-to-End Run Guide)](#9-hướng-dẫn-chạy-toàn-diện-hệ-thống-end-to-end-run-guide)
10. [Kế Hoạch Tiếp Theo (Roadmap)](#10-kế-hoạch-tiếp-theo-roadmap)

---

## 1. Giai Đoạn 1 & 2: Tái Cấu Trúc Package, POJO & Non-blocking Sink

| Hạng mục | Trước khi cập nhật | Sau khi cập nhật | Lợi ích kỹ thuật & phỏng vấn |
| :--- | :--- | :--- | :--- |
| **Package Structure** | 6 class nằm phẳng trong 1 package `spendreport`. | Phân tầng rõ ràng: `model`, `functions`, `source`, `sink` dưới namespace `com.frauddetection`. | Thoát khỏi mác bài thực hành mẫu (walkthrough template). |
| **Domain Model (`Transaction`, `Alert`)** | Phụ thuộc vào `flink-walkthrough-common`. Model `Alert` chỉ có 1 trường `id`. | Tự định nghĩa POJO chuẩn Flink. Model `Alert` có sẵn trường **`ruleId`**, `amount`, `timestamp`. | Độc lập mã nguồn, Flink `TypeSerializer` (POJO serializer) hoạt động tối ưu. |
| **Trách nhiệm I/O vs. Compute** | `FraudDetector` và CEP phải tự gọi `AlertSink.logAlert(...)` thủ công để lưu file. | Logic phát hiện chỉ emit `out.collect(new Alert(...))`. Toàn bộ I/O do `AlertFileSink` đảm nhiệm. | Tuân thủ nguyên tắc **Separation of Concerns** (Đơn trách nhiệm). |
| **Blocking File I/O** | `AlertSink` mở và đóng file trên từng alert (`new FileWriter(..., true)`). | `AlertFileSink` kế thừa `RichSinkFunction`, mở `BufferedWriter` **đúng 1 lần duy nhất** trong `open()`. | Loại bỏ nghẽn luồng worker, tối đa hóa throughput. |
| **Maven Dependencies** | Dính chặt vào `flink-walkthrough-common`. | Gỡ bỏ hoàn toàn `flink-walkthrough-common` trong `pom.xml`. Cập nhật `mainClass`. | Gọn nhẹ, sẵn sàng deploy cluster. |

---

## 2. Giai Đoạn 3: Khai Phóng Sức Mạnh Event Time & Watermark

### 2.1. Đưa Rule R2 (High Frequency) sang Tumbling Event-Time Window
- Tách R2 thành một nhánh stream độc lập trong [FraudDetectionJob.java](file:///D:/Big_Data/Fraud_Detetion_Flink/src/main/java/com/frauddetection/FraudDetectionJob.java) sử dụng `TumblingEventTimeWindows.of(Duration.ofSeconds(10))` kết hợp `WindowFunction`.
- Flink tự động gom các giao dịch theo mốc thời gian thực sự xảy ra (`t.getTimestamp()`), tự đóng mở cửa sổ và tự động giải phóng bộ nhớ khi window kết thúc.

### 2.2. Tinh gọn `FraudDetector.java`
- Đã loại bỏ `recentTimestamps` ListState và logic duyệt tần suất thủ công.
- `FraudDetector` chỉ chuyên tâm xử lý:
  - **R1: Small-then-Large:** Test thẻ nhỏ rồi rút lớn (Stateful timer).
  - **R3: Deviation from Average:** Giao dịch lệch chuẩn lịch sử chi tiêu.

### 2.3. Cấu hình Watermark Xử Lý Dữ Liệu Đến Trễ (Bounded Out-Of-Orderness)
- Sử dụng chiến lược `WatermarkStrategy.<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(5))` kèm timestamp assigner.
- Cho phép Flink chấp nhận và xử lý chính xác các giao dịch bị trễ mạng lên đến **5 giây** mà không bị loại bỏ.

### 2.4. Bổ sung Kịch bản Dữ liệu Đến Trễ trong `LabeledTransactionSource`
- Thêm nhánh phát sinh giao dịch trễ mạng 2000ms (`LATE_NORMAL`) để kiểm chứng khả năng chịu lỗi thứ tự thời gian của hệ thống.

---

## 3. Giai Đoạn 4: Quản Trị Bộ Nhớ State TTL & Phòng Chống OOM

### 3.1. Vấn đề thực tế (State Unbounded Growth)
Trong các hệ thống ngân hàng thực tế, hàng triệu tài khoản phát sinh giao dịch mỗi ngày. Nếu không có cơ chế hết hạn, State của các tài khoản ngừng giao dịch sẽ nằm vĩnh viễn trong RAM của TaskManager/RocksDB State Backend, dẫn đến rò rỉ bộ nhớ (Memory Leak) và crash hệ thống do lỗi **Out-Of-Memory (OOM)**.

### 3.2. Cấu hình `StateTtlConfig` trong [FraudDetector.java](file:///D:/Big_Data/Fraud_Detetion_Flink/src/main/java/com/frauddetection/functions/FraudDetector.java)
Trong hàm vòng đời `open(Configuration parameters)`, toàn bộ 3 State Descriptors đã được kích hoạt State TTL:

```java
StateTtlConfig ttlConfig = StateTtlConfig
        .newBuilder(Duration.ofMinutes(10))
        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
        .build();

flagDescriptor.enableTimeToLive(ttlConfig);
timerDescriptor.enableTimeToLive(ttlConfig);
amountsDescriptor.enableTimeToLive(ttlConfig);
```

---

## 4. Giai Đoạn 5: Kiểm Thử Trạng Thái (Stateful Unit Tests với Flink TestHarness)

### 4.1. Mục đích triển khai Giai đoạn 5
- Khởi tạo mini-runtime Flink trong bộ nhớ bằng `KeyedOneInputStreamOperatorTestHarness`.
- Kiểm tra tính độc lập của State giữa các tài khoản và tua nhanh thời gian kiểm thử Timer.
- **Phát hiện bug thực tế:** Bổ sung điều kiện cần tối thiểu **3 giao dịch lịch sử** (`amounts.size() >= 3`) cho Rule R3, loại bỏ hoàn toàn cảnh báo giả trên tài khoản mới mở.

### 4.2. Bộ 4 Test Cases Đã Triển Khai trong [FraudDetectorTest.java](file:///D:/Big_Data/Fraud_Detetion_Flink/src/test/java/com/frauddetection/functions/FraudDetectorTest.java)
1. `testSmallThenLarge_withinTimeout_shouldTriggerAlertR1`: Khớp đúng 1 Alert R1.
2. `testSmallThenLarge_afterTimeout_shouldNotTriggerAlert`: Timer 1s dọn cờ thành công.
3. `testDeviationFromAverage_exceedsThreshold_shouldTriggerAlertR3`: Khớp đúng 1 Alert R3.
4. `testDeviationFromAverage_normalTransaction_shouldNotTriggerAlert`: Không phát cảnh báo giả.

---

## 5. Giai Đoạn 6: Tích Hợp Apache Kafka Sink & Kiến Trúc Dual-Sink

### 5.1. Mục đích triển khai Giai đoạn 6
- Trong hệ thống Flink phân tán lớn (nhiều TaskManager), việc ghi file CSV cục bộ chỉ hoạt động tốt ở `parallelism = 1`. Để đạt chuẩn **Enterprise Cloud / Big Data Cluster**, hệ thống cần bắn dữ liệu phân tán vào **Apache Kafka** để các dịch vụ downstream (như Dashboard giám sát, Notification Service gửi SMS/Email, SIEM) có thể tiêu thụ (consume) song song.
- **Kiến trúc Dual-Sink an toàn:**
  - **Sink 1 (`AlertFileSink`):** Giữ nguyên việc ghi file `output/alerts.csv` để script [evaluate.py](file:///D:/Big_Data/Fraud_Detetion_Flink/scripts/evaluate.py) đo Precision/Recall vẫn hoạt động bình thường 100%.
  - **Sink 2 (`KafkaAlertSink`):** Đẩy thông điệp cảnh báo sang Kafka Topic dưới định dạng JSON chuẩn UTF-8.

### 5.2. Các thành phần kỹ thuật đã bổ sung:
1. **Dependency [pom.xml](file:///D:/Big_Data/Fraud_Detetion_Flink/pom.xml):**
   - Bổ sung `flink-connector-kafka` (version `3.2.0-1.19`).
2. **Lớp chuyên trách [KafkaAlertSink.java](file:///D:/Big_Data/Fraud_Detetion_Flink/src/main/java/com/frauddetection/sink/KafkaAlertSink.java):**
   - Sử dụng `KafkaSink.<Alert>builder()`.
   - Cung cấp `AlertJsonSerializationSchema` chuyển đổi POJO Alert thành payload JSON:
     `{"accountId":1,"amount":800.00,"ruleId":"R1","timestamp":1727370000}`.
3. **Cơ chế kích hoạt linh hoạt trong [FraudDetectionJob.java](file:///D:/Big_Data/Fraud_Detetion_Flink/src/main/java/com/frauddetection/FraudDetectionJob.java):**
   - Hỗ trợ cờ `--kafka` hoặc `-Dkafka.enabled=true`. Nếu không truyền cờ, hệ thống mặc định chạy chế độ File Sink an toàn, không lo crash nếu Kafka chưa bật.
4. **Hạ tầng Docker Compose [docker-compose.yml](file:///D:/Big_Data/Fraud_Detetion_Flink/docker-compose.yml):**
   - Bổ sung service `kafka` phiên bản mới nhất chạy ở chế độ **KRaft mode** (không cần Zookeeper cồng kềnh), mở cổng `9092` cho bên ngoài và `29092` cho Flink cluster nội bộ.
5. **Công cụ giám sát [scripts/kafka_consumer.py](file:///D:/Big_Data/Fraud_Detetion_Flink/scripts/kafka_consumer.py):**
   - Script Python theo dõi và in ra các cảnh báo từ Kafka topic theo thời gian thực.

---

## 6. Cấu Trúc Source Code Hiện Tại

```text
src/
├── main/
│   └── java/
│       └── com/frauddetection/
│           ├── FraudDetectionJob.java               # Pipeline Graph: Dual-Sink (File + Kafka)
│           ├── model/
│           │   ├── Transaction.java                 # POJO Giao dịch (accountId, amount, timestamp)
│           │   └── Alert.java                       # POJO Cảnh báo (accountId, amount, ruleId, timestamp)
│           ├── functions/
│           │   └── FraudDetector.java               # KeyedProcessFunction xử lý R1 & R3 kèm State TTL
│           ├── source/
│           │   └── LabeledTransactionSource.java    # Source giả lập có nhãn (Ground truth + Late arriving)
│           └── sink/
│               ├── AlertFileSink.java               # RichSinkFunction mở file 1 lần & in console
│               ├── KafkaAlertSink.java              # Flink KafkaSink đẩy Alert JSON vào Kafka
│               └── GroundTruthLogger.java           # Quản lý ghi file ground-truth.csv
└── test/
    └── java/
        └── com/frauddetection/
            └── functions/
                └── FraudDetectorTest.java           # Stateful Unit Test với KeyedOneInputStreamOperatorTestHarness
scripts/
├── evaluate.py                                      # Đo đạc Precision / Recall / F1-Score
└── kafka_consumer.py                                # Đọc và giám sát cảnh báo từ Kafka topic
docker-compose.yml                                   # Cụm phân tán: Flink (JobManager + 2 TaskManagers) + Kafka KRaft
```

---

## 7. Bảng Tổng Hợp Kỹ Thuật Theo Chuẩn Doanh Nghiệp

| Tiêu chí | Trước khi nâng cấp | Hiện tại (Sau Giai đoạn 6) | Chuẩn Flink Enterprise |
| :--- | :--- | :--- | :---: |
| **Kiến trúc phân tầng** | 1 package phẳng `spendreport` | 4 tầng: `model`, `functions`, `source`, `sink` | ✅ Đạt |
| **Mô hình Dữ liệu** | Dính `flink-walkthrough-common` | Tự chủ POJO (chuẩn `PojoSerializer`) | ✅ Đạt |
| **I/O Phân tán** | Mở/đóng file trên từng record (Blocking) | `RichSinkFunction` mở file 1 lần + **KafkaSink** | ✅ Đạt |
| **Hạ tầng Messaging** | Không có (chỉ ghi file local) | **Apache Kafka (KRaft mode)** | ✅ Đạt |
| **Event Time & Watermarks** | Chỉ dùng Processing Time | Watermark `forBoundedOutOfOrderness` (dung sai 5s) | ✅ Đạt |
| **Cửa sổ sự kiện (R2)** | Quản lý `ListState` thủ công | `TumblingEventTimeWindows` 10 giây | ✅ Đạt |
| **Chống rò rỉ bộ nhớ (OOM)** | Không có TTL (State tồn tại vĩnh viễn) | `StateTtlConfig` (10 phút, `OnCreateAndWrite`) | ✅ Đạt |
| **Kiểm thử tự động** | Thiếu hoàn toàn `src/test/java` | Stateful Unit Tests với `TestHarness` (4 test cases) | ✅ Đạt |

---

## 8. Kết Quả Biên Dịch & Chạy Test Tự Động (Verification)

Quá trình build và automated tests đã được thực thi và xác nhận:
- **Lệnh chạy Test:** `.\mvnw.cmd test`
  - Kết quả: **`Tests run: 4, Failures: 0, Errors: 0, Skipped: 0`**
  - **`BUILD SUCCESS`**
- **Lệnh đóng gói:** `.\mvnw.cmd package`
  - Kết quả: Đóng gói Uber JAR `target/frauddetection-0.1.jar` chứa đầy đủ Flink CEP và Flink Kafka Connector.
  - **`BUILD SUCCESS`**

---

## 9. Hướng Dẫn Chạy Toàn Diện Hệ Thống (End-to-End Run Guide)

### Cách 1: Chạy Chế Độ Mặc Định (File CSV & Kiểm Nghiệm Precision/Recall)
```powershell
# 1. Chạy Job Flink trực tiếp (tự động ghi file alerts.csv và ground-truth.csv):
.\mvnw.cmd compile exec:java -Dexec.mainClass="com.frauddetection.FraudDetectionJob" -Prun-locally

# 2. Đánh giá kết quả đo lường thuật toán:
python scripts/evaluate.py
```

### Cách 2: Chạy Chế Độ Phân Tán Với Kafka (Docker Cluster)
```powershell
# 1. Khởi động cụm Flink + Apache Kafka:
docker compose up -d

# 2. Mở cửa sổ Terminal 1: Lắng nghe cảnh báo từ Kafka:
python scripts/kafka_consumer.py

# 3. Mở cửa sổ Terminal 2: Chạy Flink Job với cờ kích hoạt Kafka:
.\mvnw.cmd compile exec:java -Dexec.mainClass="com.frauddetection.FraudDetectionJob" -Dexec.args="--kafka" -Prun-locally
```

---

## 10. Kế Hoạch Tiếp Theo (Roadmap)

- **Giai đoạn 7:** Dọn dẹp repo (loại bỏ các file nháp tạm `tree.txt`, `note.txt`) và cập nhật hoàn thiện `README.md` chuyên nghiệp để sẵn sàng đưa vào Portfolio / CV GitHub.
