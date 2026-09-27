# Đánh Giá & Lộ Trình Nâng Cấp Hệ Thống Fraud Detection Với Apache Flink

> **Mục tiêu tài liệu:** Báo cáo đánh giá hiện trạng dự án, phân tích khoảng cách kỹ thuật (gap analysis) giữa phiên bản PoC/Demo và chuẩn hệ thống Production-Ready, đồng thời cung cấp kiến trúc thư mục chuẩn cùng lộ trình nâng cấp dành cho nhà phát triển và các tác nhân AI (AI Coding Agents).

---

## 1. Đánh Giá Hiện Trạng Kiến Trúc

| Cấp độ | Trạng thái | Đánh giá chi tiết |
| :--- | :---: | :--- |
| **Demo / PoC / Nghiên cứu học tập** | <kbd>ĐÃ ĐÁP ỨNG TỐT</kbd> | Kế thừa chuẩn xác từ template mẫu `flink-walkthrough-datastream-java` của Apache Flink; đã bổ sung CEP, `KeyedProcessFunction`, và kịch bản thực nghiệm đo lường Precision / Recall qua script đánh giá. |
| **Production-Ready / Doanh nghiệp thực tế** | <kbd>CHƯA ĐÁP ỨNG ĐỦ</kbd> | Còn mang đặc tính của walkthrough template: cấu trúc package đơn lớp (flat), I/O ghi file cục bộ (vi phạm nguyên lý stream processing phân tán), hardcode rule tĩnh, chưa có Event Time & Watermark hoàn chỉnh, thiếu State TTL và Unit/Stateful Test. |

---

## 2. Bảng Phân Tích Lỗ Hổng Kỹ Thuật (Gap Analysis)

| Tiêu chí | Hiện trạng dự án | Chuẩn hệ thống Fraud Detection thực tế trên Flink | Mức độ ưu tiên |
| :--- | :--- | :--- | :---: |
| **Phân tầng Package** | Toàn bộ 6 class nằm phẳng trong 1 package duy nhất `spendreport`. | Phân tách các tầng trách nhiệm rõ ràng: `model`, `engine`, `rules`, `source`, `sink`, `config`, `util`. | **Cao** |
| **Mô hình Dữ liệu (Entity)** | Đang phụ thuộc vào `flink-walkthrough-common` (`Transaction`, `Alert`). | Tự định nghĩa domain model riêng với POJO tối ưu, hỗ trợ các định dạng tuần tự hóa phân tán (Avro, Protobuf, Jackson JSON) phục vụ Flink `TypeSerializer`. | **Cao** |
| **I/O Phân tán (Source & Sink)** | - `AlertSink.java` dùng `FileWriter` ghi file cục bộ đồng bộ (synchronous).<br>- `LabeledTransactionSource.java` phát sinh dữ liệu in-memory. | **Vi phạm nguyên lý streaming phân tán:** Khi Flink chạy nhiều TaskManager trên cluster, ghi file local sẽ bị phân mảnh và block worker thread.<br>→ **Chuẩn:** Dùng Kafka Source/Sink, Elasticsearch Sink, hoặc Database Sink (JDBC/Pravega). | **Rất cao** |
| **Cấu hình Rule (Rule Engine)** | Hardcode các điều kiện và ngưỡng vào code (`FraudDetector.java`, `FraudDetectionJob.java`). Mỗi lần thay đổi ngưỡng phải compile và deploy lại JAR. | Áp dụng **Broadcast State Pattern**: Rule được đưa vào từ Kafka topic / Database qua stream broadcast để cập nhật logic runtime tức thì mà không cần dừng hay khởi động lại Flink job. | **Trung bình - Cao** |
| **Event Time & State TTL** | Dùng Processing Time (`ctx.timerService().currentProcessingTime()`); State chưa thiết lập TTL (Time-To-Live). | Bắt buộc dùng **Event Time** kèm `WatermarkStrategy` để xử lý giao dịch đến trễ (late-arriving) hoặc sai thứ tự mạng; Cấu hình `StateTtlConfig` để dọn dẹp state tài khoản cũ, phòng chống Out-Of-Memory (OOM). | **Cao** |
| **Quản trị & Cấu hình** | Hardcode tham số trong Java; log dùng `slf4j-simple`. | Sử dụng `ParameterTool` / `Configuration` đọc từ `application.conf` hoặc YAML; logging qua `log4j2.properties` theo chuẩn Flink runtime. | **Trung bình** |
| **Kiểm thử (Automated Tests)** | Thiếu hoàn toàn thư mục `src/test/java`. | Bắt buộc có Unit test và Stateful Integration test sử dụng `flink-test-utils` (`KeyedOneInputStreamOperatorTestHarness`). | **Cao** |

---

## 3. Cấu Trúc Thư Mục Chuẩn Khuyến Nghị

Dưới đây là cấu trúc source code tiêu chuẩn cho một hệ thống phát hiện gian lận thời gian thực xây dựng trên nền tảng Apache Flink:

```text
Fraud_Detection_Flink/
├── docker-compose.yml              # Cluster Flink + Apache Kafka + Zookeeper/KRaft + Elasticsearch
├── pom.xml                         # Quản lý dependency (loại bỏ flink-walkthrough-common)
├── src/
│   ├── main/
│   │   ├── java/
│   │   │   └── com/company/fraud/
│   │   │       ├── FraudDetectionJob.java            # Main Graph & Pipeline wiring
│   │   │       ├── config/                           # Quản lý tham số & môi trường
│   │   │       │   ├── AppConfig.java
│   │   │       │   └── JobParameters.java
│   │   │       ├── model/                            # Data Domain Entities (POJO / Avro)
│   │   │       │   ├── Transaction.java
│   │   │       │   ├── Alert.java
│   │   │       │   ├── Rule.java                     # Dynamic broadcast rule entity
│   │   │       │   └── RulePayload.java
│   │   │       ├── source/                           # Stream Connectors đầu vào
│   │   │       │   ├── KafkaTransactionSource.java
│   │   │       │   └── DynamicRuleSource.java
│   │   │       ├── sink/                             # Stream Connectors đầu ra
│   │   │       │   ├── KafkaAlertSink.java
│   │   │       │   └── ElasticsearchAlertSink.java
│   │   │       ├── rules/                            # Tách biệt logic từng rule / CEP pattern
│   │   │       │   ├── BaseFraudEvaluator.java
│   │   │       │   ├── SmallThenLargeEvaluator.java
│   │   │       │   ├── HighFrequencyEvaluator.java
│   │   │       │   └── cep/
│   │   │       │       └── MultiStepPatternBuilder.java
│   │   │       ├── functions/                        # Core Flink Stream Operators
│   │   │       │   ├── DynamicFraudDetector.java     # KeyedBroadcastProcessFunction
│   │   │       │   └── TransactionWatermarkStrategy.java
│   │   │       └── util/                             # Serde (JSON/Avro) và metrics helpers
│   │   │           └── JsonSerdeUtils.java
│   │   └── resources/
│   │       ├── application.conf                      # Cấu hình Kafka brokers, checkpoint, parallelisms
│   │       └── log4j2.properties                     # Chuẩn logging production
│   └── test/
│       └── java/
│           └── com/company/fraud/
│               ├── rules/
│               │   └── SmallThenLargeTest.java       # Unit test logic phát hiện
│               └── functions/
│                   └── FraudDetectorHarnessTest.java # Stateful test cho State & Timer với TestHarness
└── scripts/
    ├── evaluate.py                                   # Script tính toán ma trận nhầm lẫn & F1-score
    └── mock_producer.py                              # Giả lập transaction stream đẩy vào Kafka
```

---

## 4. Lộ Trình Nâng Cấp Kỹ Thuật (Action Plan)

> [!NOTE]
> - **Nếu mục tiêu là hoàn thành bài tập lớn / báo cáo thực nghiệm:** Cấu trúc hiện tại trong package `spendreport` đã đủ đáp ứng yêu cầu kỹ thuật thực nghiệm và tiện cho việc chạy script [evaluate.py](file:///D:/Big_Data/Fraud_Detetion_Flink/evaluate.py).
> - **Nếu mục tiêu là làm đồ án tốt nghiệp, xây dựng sản phẩm thực tế hoặc đưa vào Portfolio/CV:** Cần thực hiện các bước nâng cấp trọng tâm dưới đây.

### Giai đoạn 1: Chuẩn hóa Model & Package (Refactoring Core)
- [ ] **Tách biệt Package:** Đổi tên base package từ `spendreport` sang cấu trúc chuẩn (ví dụ `com.company.fraud` hoặc `org.example.fraud`).
- [ ] **Độc lập hóa Domain Model:** Tự định nghĩa các lớp POJO `Transaction` và `Alert` với đầy đủ getters, setters, `equals`, `hashCode`, `toString`.
- [ ] **Loại bỏ dependency thừa:** Xóa bỏ `flink-walkthrough-common` trong [pom.xml](file:///D:/Big_Data/Fraud_Detetion_Flink/pom.xml).

### Giai đoạn 2: Chuẩn hóa I/O & Streaming Phân Tán
- [ ] **Thay thế File I/O:** Chuyển `AlertSink.java` sang sử dụng `KafkaSink<Alert>` (`org.apache.flink.connector.kafka.sink.KafkaSink`).
- [ ] **Kafka Input Source:** Viết connector đọc luồng giao dịch từ Kafka topic bằng `KafkaSource<Transaction>`.
- [ ] **Event Time & Watermarks:** Cấu hình `WatermarkStrategy.forBoundedOutOfOrderness(...)` dựa trên timestamp của giao dịch.

### Giai đoạn 3: Tối Ưu State Management & Dynamic Rules
- [ ] **State Time-To-Live (TTL):** Thiết lập `StateTtlConfig` cho `ValueState<Boolean>` hoặc `ListState` để giải phóng bộ nhớ TaskManager.
- [ ] **Broadcast State Pattern:** Triển khai luồng cấu hình Rule động qua `BroadcastStream` kết hợp với `KeyedBroadcastProcessFunction`.

### Giai đoạn 4: Đảm Bảo Chất Lượng & Kiểm Thử Tự Động
- [ ] **Unit Tests:** Kiểm thử độc lập logic của các evaluator và pattern rules.
- [ ] **Stateful Tests:** Viết integration test sử dụng `KeyedOneInputStreamOperatorTestHarness` để kiểm thử chính xác cơ chế timer, state checkpoint và recovery.
- [ ] **Logging & Config:** Chuyển các tham số cấu hình ra file `application.conf` hoặc biến môi trường; cấu hình logging thông qua `log4j2.properties`.

---

## 5. Hướng Dẫn Dành Cho Tác Nhân AI (AI Prompt Guidelines)

Khi yêu cầu AI thực hiện tái cấu trúc hoặc sinh mã nguồn cho dự án này, hãy sử dụng các nguyên tắc sau:
1. **Serialization:** Luôn đảm bảo tất cả model tuân thủ chuẩn Flink POJO (có constructor rỗng, các trường là public hoặc có getter/setter chuẩn).
2. **Determinism:** State và Timers trong Flink phải xác định; ưu tiên dùng `Event Time` thay cho `Processing Time`.
3. **No Blocking I/O:** Tuyệt đối không gọi blocking synchronous I/O (như ghi file local, HTTP sync request) bên trong các hàm xử lý stream (`ProcessFunction`, `MapFunction`).
