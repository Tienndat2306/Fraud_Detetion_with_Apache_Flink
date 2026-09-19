# HƯỚNG DẪN CÀI ĐẶT THƯ VIỆN, MÔI TRƯỜNG VÀ CHẠY DỰ ÁN FRAUD DETECTION (APACHE FLINK)

Tài liệu này hướng dẫn chi tiết từng bước từ cài đặt các công cụ cần thiết, cấu hình môi trường, build dự án đến các cách chạy hệ thống phát hiện gian lận thẻ (Fraud Detection) và đánh giá kết quả (Precision, Recall, F1).

---

## MỤC LỤC

1. [Yêu cầu hệ thống và Công cụ cần chuẩn bị](#1-yêu-cầu-hệ-thống-và-công-cụ-cần-chuẩn-bị)
2. [Cài đặt và Cấu hình môi trường](#2-cài-đặt-và-cấu-hình-môi-trường)
   - [2.1. Cài đặt Java JDK (11 hoặc 17)](#21-cài-đặt-java-jdk-11-hoặc-17)
   - [2.2. Cài đặt Maven (Tùy chọn - Dự án đã tích hợp sẵn Maven Wrapper)](#22-cài-đặt-maven-tùy-chọn)
   - [2.3. Cài đặt Python và thư viện Pandas](#23-cài-đặt-python-và-thư-viện-pandas)
   - [2.4. Cài đặt Docker &amp; Docker Compose (Tùy chọn - Chạy cụm Flink)](#24-cài-đặt-docker--docker-compose)
3. [Cấu trúc mã nguồn của dự án](#3-cấu-trúc-mã-nguồn-của-dự-án)
4. [Hướng dẫn Build dự án (Đóng gói mã nguồn)](#4-hướng-dẫn-build-dự-án-đóng-gói-mã-nguồn)
5. [Các cách chạy chương trình](#5-các-cách-chạy-chương-trình)
   - [Cách 1: Chạy trực tiếp trong IntelliJ IDEA / VS Code (Khuyến nghị để kiểm thử &amp; debug)](#cách-1-chạy-trực-tiếp-trong-intellij-idea--vs-code)
   - [Cách 2: Chạy bằng dòng lệnh Java (Command Line)](#cách-2-chạy-bằng-dòng-lệnh-java-command-line)
   - [Cách 3: Triển khai trên Cụm Flink phân tán bằng Docker Compose](#cách-3-triển-khai-trên-cụm-flink-phân-tán-bằng-docker-compose)
6. [Hướng dẫn Đánh giá kết quả (Precision, Recall, F1)](#6-hướng-dẫn-đánh-giá-kết-quả-precision-recall-f1)
7. [Xử lý các sự cố thường gặp (Troubleshooting)](#7-xử-lý-các-sự-cố-thường-gặp-troubleshooting)

---

## 1. Yêu cầu hệ thống và Công cụ cần chuẩn bị

| Công cụ / Module                | Phiên bản khuyến nghị                              | Mục đích                                                             |
| --------------------------------- | ------------------------------------------------------ | ----------------------------------------------------------------------- |
| **Java JDK**                | **Java 11** hoặc **Java 17** (LTS)        | Biên dịch và thực thi Apache Flink 1.20                             |
| **Apache Maven**            | 3.6.x trở lên*(Đã tích hợp sẵn `mvnw.cmd`)* | Quản lý dependencies và build gói`.jar`                           |
| **Python**                  | 3.8+                                                   | Chạy script tính toán độ chính xác và thống kê                |
| **Pandas** (Python library) | 1.3+ trở lên                                         | Đọc CSV, xử lý khớp nhãn ground-truth và tính Precision/Recall  |
| **Docker & Docker Compose** | Docker Desktop mới nhất                              | Triển khai cụm Flink cluster thực tế (1 JobManager, 2 TaskManagers) |
| **IDE**                     | IntelliJ IDEA (khuyến nghị) hoặc VS Code            | Viết code, theo dõi log và debug                                     |

---

## 2. Cài đặt và Cấu hình môi trường

### 2.1. Cài đặt Java JDK (11 hoặc 17)

Apache Flink 1.20 hoạt động ổn định và tương thích tốt nhất với **Java 11** hoặc **Java 17**.

1. **Tải JDK:**
   - Tải Eclipse Temurin (OpenJDK): [https://adoptium.net/temurin/releases/?version=17](https://adoptium.net/temurin/releases/?version=17) (hoặc Java 11).
   - Hoặc tải Oracle JDK: [https://www.oracle.com/java/technologies/downloads/#java17](https://www.oracle.com/java/technologies/downloads/#java17).
2. **Cấu hình biến môi trường trên Windows:**
   - Mở **System Properties** -> **Environment Variables**.
   - Tạo biến hệ thống (System variable) mới:
     - Tên: `JAVA_HOME`
     - Giá trị: Đường dẫn tới thư mục cài JDK, ví dụ: `C:\Program Files\Eclipse Adoptium\jdk-17.0.x` hoặc `C:\Program Files\Java\jdk-17`.
   - Tìm biến `Path`, bấm **Edit**, thêm dòng: `%JAVA_HOME%\bin`.
3. **Kiểm tra cài đặt:**
   Mở Command Prompt hoặc PowerShell mới và gõ:
   ```cmd
   java -version
   ```

   *Kết quả cần hiển thị phiên bản Java 11 hoặc 17.*

---

### 2.2. Cài đặt Maven (Tùy chọn)

> **Lưu ý:** Trong thư mục `frauddetection` đã có sẵn **Maven Wrapper (`mvnw.cmd`)**, script này sẽ tự động tải Maven 3.9.6 về máy khi chạy lần đầu, bạn **không bắt buộc** phải cài đặt Maven thủ công.

Nếu bạn muốn cài đặt Maven độc lập trên máy:

1. Tải bản binary zip: [https://maven.apache.org/download.cgi](https://maven.apache.org/download.cgi) (ví dụ `apache-maven-3.9.6-bin.zip`).
2. Giải nén vào thư mục, ví dụ: `C:\tools\apache-maven-3.9.6`.
3. Thêm biến môi trường `MAVEN_HOME` trỏ tới thư mục trên và thêm `%MAVEN_HOME%\bin` vào `Path`.
4. Kiểm tra:
   ```cmd
   mvn -version
   ```

---

### 2.3. Cài đặt Python và thư viện Pandas

Script `evaluate.py` dùng để đối chiếu kết quả cảnh báo (`alerts.csv`) với nhãn gốc (`ground-truth.csv`).

1. **Kiểm tra Python:**
   ```cmd
   python --version
   ```
2. **Cài đặt thư viện Pandas:**
   Mở terminal và chạy lệnh:
   ```cmd
   pip install pandas
   ```

   *(Nếu bạn dùng nhiều phiên bản Python, có thể dùng `python -m pip install pandas`)*.

---

### 2.4. Cài đặt Docker & Docker Compose (Tùy chọn)

Dùng để giả lập cụm Flink phân tán giống môi trường sản xuất thực tế:

1. Tải và cài đặt **Docker Desktop for Windows**: [https://www.docker.com/products/docker-desktop/](https://www.docker.com/products/docker-desktop/).
2. Đảm bảo WSL 2 backend đã được bật trong cài đặt Docker Desktop.
3. Kiểm tra trong terminal:
   ```cmd
   docker --version
   docker compose version
   ```

---

## 3. Cấu trúc mã nguồn của dự án

Toàn bộ mã nguồn dự án nằm tại thư mục gốc `D:\Big_Data\Fraud_Detetion_Flink`:

```
D:\Big_Data\Fraud_Detetion_Flink\
├── pom.xml                               # Định nghĩa dependencies (Flink 1.20, Flink CEP, Shade Plugin)
├── mvnw.cmd                              # Maven Wrapper cho Windows (chạy build không cần cài Maven)
├── .mvn/wrapper/                         # Bộ nạp maven-wrapper
├── docker-compose.yml                    # Cấu hình cụm Flink (1 JobManager + 2 TaskManagers)
├── evaluate.py                           # Script Python đánh giá Precision, Recall, F1
├── target/
│   └── frauddetection-0.1.jar            # File JAR (Fat/Uber JAR) chứa toàn bộ code và dependencies
├── output/                               # Thư mục tự động sinh ra khi chạy Job
│   ├── ground-truth.csv                  # Dữ liệu gốc với nhãn thật (expectedLabel: 0 hoặc 1)
│   └── alerts.csv                        # Cảnh báo gian lận thực tế mà hệ thống phát hiện
└── src/main/java/spendreport/
    ├── FraudDetectionJob.java            # Main Job: kết nối Source -> ProcessFunction (R1, R2, R3) + CEP (R4) -> Sink
    ├── FraudDetector.java                # KeyedProcessFunction: Triển khai 3 rule R1, R2, R3 với Flink State & Timer
    ├── LabeledTransactionSource.java     # Nguồn phát sinh giao dịch có nhãn (Ground Truth Generator)
    ├── GroundTruthLogger.java            # Module ghi log giao dịch gốc ra ground-truth.csv
    ├── AlertSink.java                    # Module ghi nhận các cảnh báo ra alerts.csv
    └── AlertSinkFunction.java            # Flink Sink hiển thị alert ra console chuẩn
```

---

## 4. Hướng dẫn Build dự án (Đóng gói mã nguồn)

Mở PowerShell hoặc Command Prompt tại thư mục gốc:

```powershell
cd D:\Big_Data\Fraud_Detetion_Flink
```

### Cách 1: Build bằng Maven (nếu máy đã có sẵn `mvn`)

```powershell
mvn clean package -DskipTests
```

### Cách 2: Build bằng Maven Wrapper (khuyên dùng nếu chưa cài Maven)

```powershell
.\mvnw.cmd clean package -DskipTests
```

*(Nếu gặp cảnh báo về multiModuleProjectDirectory trên một số phiên bản Java, bạn có thể chạy:)*

```powershell
java "-Dmaven.multiModuleProjectDirectory=D:\Big_Data\Fraud_Detetion_Flink" -classpath ".mvn\wrapper\maven-wrapper.jar" org.apache.maven.wrapper.MavenWrapperMain clean package -DskipTests
```

**Kết quả sau khi build thành công:**

- Xuất hiện file: `target/frauddetection-0.1.jar`
- File JAR này là **Fat/Uber JAR** đã được bundle sẵn Flink CEP, Walkthrough common entities, SLF4J logger,... sẵn sàng để chạy trực tiếp hoặc submit lên Flink Cluster.

---

## 5. Các cách chạy chương trình

### Cách 1: Chạy trực tiếp trong IntelliJ IDEA / VS Code (Khuyến nghị)

Đây là cách nhanh nhất và thuận tiện nhất để chạy demo, quan sát log theo thời gian thực và debug code:

1. **Mở dự án:**
   - Mở IntelliJ IDEA -> Chọn **Open** -> Chọn thư mục gốc `D:\Big_Data\Fraud_Detetion_Flink` (hoặc mở file `pom.xml` dưới dạng Open as Project).
2. **Cấu hình Project SDK:**
   - Vào **File** -> **Project Structure** -> **Project** -> Chọn SDK là **JDK 11** hoặc **JDK 17**.
3. **Cấu hình Run Configuration (Rất quan trọng trong Flink):**
   - Mở file `src/main/java/spendreport/FraudDetectionJob.java`.
   - Chuột phải vào `main()` chọn **Modify Run Configuration...** (hoặc **Edit Configurations**).
   - Tích chọn ô: **"Add dependencies with 'provided' scope to classpath"** (do các dependencies của Flink được đặt là `provided` để tối ưu kích thước khi deploy cluster).
   - Bấm **Apply** -> **OK**.
4. **Chạy ứng dụng:**
   - Bấm nút **Run** (biểu tượng tam giác xanh) tại file `FraudDetectionJob.java`.
   - Trên cửa sổ Console bạn sẽ thấy:
     - Các giao dịch được phát sinh.
     - Các cảnh báo gian lận `[ALERT R1]`, `[ALERT R2]`, `[ALERT R3]`, `[ALERT R4]` xuất hiện ngay lập tức khi vi phạm rule.
     - Tự động tạo thư mục `output/` chứa 2 file `ground-truth.csv` và `alerts.csv`.

---

### Cách 2: Chạy bằng dòng lệnh Maven Wrapper (CLI Local)

> **💡 Lưu ý về cơ chế `<scope>provided</scope>` trong Apache Flink:**
> Các thư viện nòng cốt của Flink (`flink-streaming-java`, `flink-clients`) được cấu hình `<scope>provided</scope>` trong `pom.xml`. Đây là chuẩn của Apache Flink giúp file JAR đóng gói nhẹ và tránh xung đột thư viện khi submit lên Flink Cluster.
> Do đó, file JAR `frauddetection-0.1.jar` không chứa sẵn runtime Flink, nếu chạy trực tiếp bằng `java -jar` sẽ gặp lỗi `NoClassDefFoundError: SourceFunction`.
> Để chạy kiểm thử ngay trên máy local bằng dòng lệnh với đầy đủ runtime Flink MiniCluster, bạn sử dụng lệnh `exec:java` qua Maven Wrapper:

```powershell
cd D:\Big_Data\Fraud_Detetion_Flink
.\set-env.ps1
.\mvnw.cmd exec:java "-Dexec.mainClass=spendreport.FraudDetectionJob" "-Dexec.classpathScope=test"
```

Job sẽ tự động khởi động **Flink Mini Cluster** ngay trên máy, thực thi bộ sinh giao dịch `LabeledTransactionSource`, xử lý 4 rule phát hiện gian lận và tự động ghi kết quả vào thư mục `output/`.

---

### Cách 3: Triển khai trên Cụm Flink phân tán bằng Docker Compose

Nếu bạn muốn mô phỏng hệ thống chạy trên cụm Flink phân tán thực thụ (phục vụ mục kiểm thử hiệu năng TN5, TN6 và chụp màn hình Flink Web Dashboard):

1. **Khởi động Flink Cluster:**
   ```powershell
   cd D:\Big_Data\Fraud_Detetion_Flink
   docker compose up -d
   ```
2. **Kiểm tra trạng thái:**
   - Mở trình duyệt web và truy cập Dashboard của Flink: **[http://localhost:8081](http://localhost:8081)**
   - Bạn sẽ thấy Flink Web UI với **1 JobManager** và **2 TaskManagers**, các Slot khả dụng.
3. **Submit Job vào cụm:**
   - **Cách 3.1: Dùng giao diện Web UI:**
     1. Truy cập `http://localhost:8081`.
     2. Bấm vào menu **Submit New Job** ở cột bên trái.
     3. Bấm **+ Add Jar**, chọn file `target/frauddetection-0.1.jar`.
     4. Chọn file vừa tải lên, bấm **Submit**.
   - **Cách 3.2: Dùng dòng lệnh:**
     Copy file jar vào container JobManager và submit:
     ```powershell
     docker cp target/frauddetection-0.1.jar fraud_detetion_flink-jobmanager-1:/opt/flink/
     docker exec -it fraud_detetion_flink-jobmanager-1 flink run /opt/flink/frauddetection-0.1.jar
     ```
4. **Theo dõi:**
   - Xem đồ thị luồng xử lý (DAG Topology), throughput (records/s), latency và checkpoints trực tiếp trên Web UI.
5. **Tắt cụm sau khi hoàn thành:**
   ```powershell
   docker compose down
   ```

---

## 6. Hướng dẫn Đánh giá kết quả (Precision, Recall, F1)

Sau khi chạy xong chương trình (ở Bước 5), trong thư mục `output/` sẽ có 2 file dữ liệu:

- `ground-truth.csv`: Danh sách toàn bộ giao dịch đã phát, kèm cờ `expectedLabel` (1: Thực sự là gian lận, 0: Bình thường/Nhiễu).
- `alerts.csv`: Danh sách các cảnh báo mà 4 Rule của hệ thống Flink đã bắt được kèm `ruleId` (`R1`, `R2`, `R3`, `R4`).

### Chạy script đánh giá:

Mở terminal tại thư mục gốc:

```powershell
cd D:\Big_Data\Fraud_Detetion_Flink
python scripts/evaluate.py
```

### Mẫu kết quả hiển thị trên màn hình:

```text
============================================================
  FRAUD DETECTION EVALUATION
  Đánh giá hiệu quả phát hiện gian lận
============================================================

Loaded ground truth: 2000 records
  - Fraud (label=1): 450
  - Normal (label=0): 1550
Loaded alerts: 432 records

==================================================
  Overall Results
==================================================
  True Positives  (TP): 428
  False Positives (FP): 4
  False Negatives (FN): 22
  True Negatives  (TN): 1546
--------------------------------------------------
  Precision: 0.9907
  Recall:    0.9511
  F1-Score:  0.9705
==================================================

==================================================
  Results for Rule: R1
==================================================
  ...
==================================================
  Results for Rule: R2
==================================================
  ...
```

### Ý nghĩa các chỉ số:

- **TP (True Positive):** Giao dịch gian lận thực tế và hệ thống đã phát hiện chính xác.
- **FP (False Positive):** Giao dịch bình thường hoặc nhiễu nhưng hệ thống báo nhầm là gian lận (Báo động giả).
- **FN (False Negative):** Giao dịch gian lận bị bỏ lọt.
- **TN (True Negative):** Giao dịch bình thường và hệ thống không đưa ra cảnh báo.
- **Precision (Độ chuẩn xác):** $\frac{TP}{TP + FP}$ — Tỷ lệ cảnh báo đúng trên tổng số cảnh báo phát ra.
- **Recall (Độ bao phủ):** $\frac{TP}{TP + FN}$ — Tỷ lệ bắt được gian lận trên tổng số gian lận xảy ra.
- **F1-Score:** Trung bình điều hòa giữa Precision và Recall.

---

## 7. Xử lý các sự cố thường gặp (Troubleshooting)

### Sự cố 1: `mvn` không được nhận diện là lệnh hợp lệ

- **Nguyên nhân:** Chưa cài Maven hoặc chưa cấu hình biến môi trường `PATH`.
- **Cách xử lý:** Sử dụng trực tiếp script `.\mvnw.cmd clean package -DskipTests` có sẵn trong thư mục dự án thay vì lệnh `mvn`.

### Sự cố 2: IntelliJ báo lỗi đỏ `ClassNotFoundException: org.apache.flink...` khi Run

- **Nguyên nhân:** Các thư viện Flink trong file `pom.xml` được cấu hình scope `<scope>provided</scope>` để phục vụ việc đóng gói nhẹ khi đưa lên cluster.
- **Cách xử lý:** Trong IntelliJ, vào **Run** -> **Edit Configurations...** -> chọn configuration của `FraudDetectionJob` -> tích chọn **"Add dependencies with 'provided' scope to classpath"** -> **Apply**.

### Sự cố 3: Không tìm thấy file `output/ground-truth.csv` khi chạy `evaluate.py`

- **Nguyên nhân:**
  1. Chưa chạy `FraudDetectionJob` trước đó, hoặc
  2. Terminal chạy `evaluate.py` không đứng tại đúng thư mục gốc `D:\Big_Data\Fraud_Detetion_Flink`.
- **Cách xử lý:**
  - Hãy chắc chắn bạn đã `cd D:\Big_Data\Fraud_Detetion_Flink`.
  - Chạy `FraudDetectionJob` ít nhất 10-20 giây để hệ thống kịp sinh giao dịch và lưu vào thư mục `output/`.

### Sự cố 4: Xung đột cổng 8081 khi chạy Docker Compose

- **Nguyên nhân:** Cổng 8081 đang bị ứng dụng khác trên máy chiếm dụng.
- **Cách xử lý:** Trong file `docker-compose.yml`, đổi cổng `ports: ["8081:8081"]` thành `ports: ["8082:8081"]`, sau đó truy cập Web UI tại `http://localhost:8082`.

---

*Tài liệu được biên soạn đồng bộ với mã nguồn triển khai thực tế của dự án.*
