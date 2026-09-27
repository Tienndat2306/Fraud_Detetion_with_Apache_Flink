# Hướng dẫn nâng cấp dự án thành chuẩn Portfolio/CV

**Cơ sở của tài liệu này:** đọc trực tiếp từ mã nguồn thực tế trong `Fraud_Detetion_Flink.zip` (không suy đoán) và đối chiếu với `improve.md`. Khác với `improve.md` (liệt kê đầy đủ chuẩn doanh nghiệp), tài liệu này **ưu tiên theo giá trị/chi phí cho mục tiêu portfolio** — không nhất thiết phải làm 100% để "khoe" trong CV/phỏng vấn.

## Ghi nhận trước: bạn đã làm tốt hơn improve.md mô tả

`improve.md` liệt kê "chưa có Event Time & Watermark" và ngụ ý chưa bật checkpoint — nhưng đọc `FraudDetectionJob.java` thực tế cho thấy:
- ✅ `env.enableCheckpointing(30000)` **đã bật từ trước**.
- ✅ `WatermarkStrategy.forMonotonousTimestamps()` với `withTimestampAssigner` **đã gán** cho luồng `transactions`.

→ Vấn đề thật sự không phải "chưa có Event Time" mà là: **đã gán watermark nhưng chưa dùng nó** — cả 3 rule trong `FraudDetector.java` vẫn gọi `ctx.timerService().currentProcessingTime()` thay vì dùng Event Time timer. Đây là khoảng trống cụ thể cần sửa (mục Giai đoạn 3 bên dưới), không phải làm từ số 0.

---

## Nguyên tắc chọn việc: Portfolio ≠ Production

`improve.md` đúng về mặt kỹ thuật nhưng nhắm tới **chuẩn doanh nghiệp** (Kafka + Elasticsearch + Broadcast State + Avro...). Với mục tiêu portfolio/CV, mục tiêu là **chứng minh bạn hiểu và làm chủ được các khái niệm cốt lõi của Flink**, không phải dựng một hệ thống vận hành thật. Bảng dưới đây phân loại lại theo mức độ nên làm:

| Việc | Giá trị cho portfolio | Chi phí | Kết luận |
|---|---|---|---|
| Tách package theo tầng | Cao (code dễ đọc khi nhà tuyển dụng mở repo) | Thấp | **Làm** |
| Tự viết POJO domain model | Trung bình-cao (thoát khỏi "chỉ là bài mẫu") | Thấp | **Làm** |
| Sửa I/O blocking trong operator | Cao (đây là lỗi thiết kế thật, không phải lý thuyết suông) | Thấp | **Làm** |
| Event Time thực sự cho rule | Cao (nhất quán với lý thuyết đã trình bày) | Trung bình | **Làm** |
| State TTL | Trung bình (dễ giải thích, dễ làm) | Thấp | **Làm** |
| Unit + Stateful Test | Cao (rất hay bị hỏi khi phỏng vấn Data Engineer) | Trung bình | **Làm** |
| Kafka Source/Sink | Trung bình (đẹp nhưng không bắt buộc) | Cao | Tùy thời gian còn lại |
| Elasticsearch Sink | Thấp cho portfolio cá nhân | Cao | Bỏ qua |
| Broadcast State (dynamic rule) | Cao nếu làm xong, nhưng rủi ro dở dang | Rất cao | Chỉ làm nếu dư thời gian |
| application.conf / log4j2 | Thấp-trung bình | Thấp | Làm nhanh cuối cùng nếu rảnh |

---

## Giai đoạn 1 — Tái cấu trúc Package & Domain Model

### 1.1. Cấu trúc package mới (rút gọn so với improve.md, vẫn đủ chuyên nghiệp)

Không cần đủ 8 tầng như `improve.md` đề xuất — với quy mô 6 class, tách 4 tầng là đủ chuyên nghiệp mà không bị over-engineering:

```
src/main/java/com/tiendat/frauddetection/
├── FraudDetectionJob.java
├── model/
│   ├── Transaction.java        # POJO tự viết, thay flink-walkthrough-common
│   └── Alert.java              # POJO tự viết, có thêm field ruleId (Alert gốc không có!)
├── functions/
│   └── FraudDetector.java
├── source/
│   └── LabeledTransactionSource.java
└── sink/
    ├── AlertFileSink.java      # gộp AlertSink + AlertSinkFunction hiện tại thành 1 RichSinkFunction
    └── GroundTruthFileSink.java
```

> Đổi `spendreport` → package theo tên bạn (ví dụ `com.tiendat.frauddetection`) — đây chính là việc `improve.md` gọi là "thoát khỏi walkthrough template", chi phí chỉ là Find & Replace.

### 1.2. Tự viết POJO thay `flink-walkthrough-common`

Đây là thay đổi quan trọng nhất để "cắt dây" khỏi bản mẫu gốc. `Alert` gốc chỉ có 1 field `id` — đó là lý do hiện tại bạn phải ghi `ruleId` qua một cơ chế phụ (`AlertSink.logAlert` gọi tay trong từng rule) thay vì để nó là một phần của model:

```java
package com.tiendat.frauddetection.model;

public class Transaction {
    private long accountId;
    private double amount;
    private long timestamp;

    public Transaction() {} // bắt buộc: constructor rỗng cho Flink POJO

    public long getAccountId() { return accountId; }
    public void setAccountId(long accountId) { this.accountId = accountId; }
    public double getAmount() { return amount; }
    public void setAmount(double amount) { this.amount = amount; }
    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

    @Override
    public String toString() {
        return String.format("Transaction{accountId=%d, amount=%.2f, ts=%d}",
                accountId, amount, timestamp);
    }
}
```

```java
package com.tiendat.frauddetection.model;

public class Alert {
    private long accountId;
    private double amount;
    private String ruleId;   // <-- field mới, loại bỏ việc gọi AlertSink.logAlert() tay
    private long timestamp;

    public Alert() {}

    public Alert(long accountId, double amount, String ruleId, long timestamp) {
        this.accountId = accountId;
        this.amount = amount;
        this.ruleId = ruleId;
        this.timestamp = timestamp;
    }

    public long getAccountId() { return accountId; }
    public void setAccountId(long accountId) { this.accountId = accountId; }
    public double getAmount() { return amount; }
    public void setAmount(double amount) { this.amount = amount; }
    public String getRuleId() { return ruleId; }
    public void setRuleId(String ruleId) { this.ruleId = ruleId; }
    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
}
```

Sau khi có `Alert.ruleId` là field thật, mọi nơi trong `FraudDetector`/`FraudDetectionJob` đang viết kiểu:
```java
Alert alert = new Alert();
alert.setId(t.getAccountId());
out.collect(alert);
AlertSink.logAlert(t.getAccountId(), t.getAmount(), "R1", t.getTimestamp()); // gọi tay riêng
```
→ rút gọn thành:
```java
out.collect(new Alert(t.getAccountId(), t.getAmount(), "R1", t.getTimestamp()));
```
Việc ghi file chuyển hẳn xuống sink (xem Giai đoạn 2) — logic phát hiện không còn "biết" gì về việc ghi file nữa, đúng nguyên tắc tách trách nhiệm (separation of concerns) — đây chính là điểm mà một reviewer có kinh nghiệm sẽ đánh giá cao khi đọc code.

Trong `pom.xml`, xóa dependency:
```xml
<!-- XÓA dòng này sau khi đã tự viết POJO -->
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-walkthrough-common</artifactId>
    <version>${flink.version}</version>
</dependency>
```

---

## Giai đoạn 2 — Sửa lỗi I/O chặn luồng (quan trọng hơn improve.md mô tả)

`improve.md` mô tả đây là "vi phạm nguyên lý phân tán" ở mức lý thuyết chung, nhưng đọc code thật cho thấy vấn đề **cụ thể và nghiêm trọng hơn** improve.md nói:

Trong `AlertSink.java` hiện tại:
```java
try (PrintWriter writer = new PrintWriter(new FileWriter(FILE_PATH, true))) {
    ...
}
```
→ **Mở và đóng file trên MỖI LẦN gọi** (mỗi 1 alert = 1 lần open+close file). Với `GroundTruthLogger`, tệ hơn: **mỗi 1 trong 2000 giao dịch = 1 lần mở/đóng file**, ngay trong thread của `LabeledTransactionSource.emit()`. Đây là I/O đồng bộ (blocking) chạy ngay trong luồng dữ liệu chính — không cần Kafka mới sửa được, chỉ cần chuyển sang **1 sink mở file một lần duy nhất**.

### 2.1. Sửa bằng RichSinkFunction với buffered writer mở 1 lần (không cần Kafka)

```java
package com.tiendat.frauddetection.sink;

import com.tiendat.frauddetection.model.Alert;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Locale;

public class AlertFileSink extends RichSinkFunction<Alert> {

    private static final String FILE_PATH = "output/alerts.csv";
    private transient BufferedWriter writer;

    @Override
    public void open(Configuration parameters) throws Exception {
        Files.createDirectories(Paths.get("output"));
        writer = new BufferedWriter(new FileWriter(FILE_PATH, false)); // mở đúng 1 lần
        writer.write("accountId,amount,ruleId,timestamp");
        writer.newLine();
    }

    @Override
    public void invoke(Alert alert, Context context) throws Exception {
        writer.write(String.format(Locale.US, "%d,%.2f,%s,%d",
                alert.getAccountId(), alert.getAmount(), alert.getRuleId(), alert.getTimestamp()));
        writer.newLine();
        writer.flush(); // flush để evaluate.py đọc được ngay cả khi job đang chạy
    }

    @Override
    public void close() throws Exception {
        if (writer != null) writer.close();
    }
}
```

Áp dụng tương tự cho `GroundTruthFileSink` (viết dưới dạng `SourceFunction` nội bộ hoặc tách riêng thành 1 sink độc lập nếu bạn tách ground-truth logging ra khỏi Source — xem lưu ý bên dưới).

> **Lưu ý về giới hạn:** cách này vẫn ghi file cục bộ, nên **chỉ chạy đúng với `parallelism = 1`** (nếu tăng song song, nhiều subtask sẽ tranh nhau ghi cùng 1 file → hỏng dữ liệu). Đây là giới hạn nên **ghi rõ trong README** như một "known limitation" — điều này bản thân nó là một điểm cộng phỏng vấn (bạn biết giới hạn của thiết kế mình chọn, và biết Kafka Sink là hướng khắc phục đúng nếu cần chạy đa song song).

### 2.2. Ground-truth logging: tách khỏi Source thay vì gọi trực tiếp trong `emit()`

Hiện tại `LabeledTransactionSource.emit()` vừa phát dữ liệu vừa tự ghi file — 2 trách nhiệm trong 1 chỗ. Cách sạch hơn: để Source chỉ phát `Transaction`, còn việc ghi ground-truth chuyển thành một `ProcessFunction` riêng gắn thêm vào luồng chính (side output), hoặc đơn giản hơn cho quy mô hiện tại: giữ nguyên cơ chế gọi trực tiếp nhưng đổi `GroundTruthLogger` sang cùng kiểu buffered-writer-mở-1-lần như `AlertFileSink` ở trên (đủ để giải quyết vấn đề hiệu năng, không bắt buộc phải tách hẳn kiến trúc).

---

## Giai đoạn 3 — Dùng Event Time thật cho ít nhất 1 rule

Bạn đã có watermark, chỉ cần **một** rule chuyển từ Processing Time sang Event Time để chứng minh hiểu sự khác biệt. **R2 (High Frequency)** là ứng viên tốt nhất vì đang dùng cửa sổ thời gian (`R2_WINDOW`) — rất tự nhiên để chuyển sang Event-Time Window thay vì tự quản lý `ListState` thời gian bằng tay:

```java
// Thay vì tự quản lý ListState<Long> recentTimestamps trong FraudDetector,
// tách R2 thành 1 nhánh riêng dùng Tumbling Event-Time Window:

DataStream<Alert> r2Alerts = transactions
        .keyBy(Transaction::getAccountId)
        .window(TumblingEventTimeWindows.of(Time.seconds(10)))
        .apply((key, window, values, out) -> {
            int count = 0;
            for (Transaction t : values) count++;
            if (count > FREQ_THRESHOLD) {
                Transaction last = null;
                for (Transaction t : values) last = t;
                out.collect(new Alert(key, last.getAmount(), "R2", window.getEnd()));
            }
        });
```

Việc này giúp R2 tự động xử lý đúng theo **thời điểm giao dịch thực sự xảy ra** (`t.getTimestamp()`) thay vì thời điểm hệ thống xử lý nó — khác biệt rõ nhất khi mô phỏng thêm dữ liệu đến trễ/sai thứ tự (xem Giai đoạn 3.1).

### 3.1. (Điểm cộng mạnh) Thêm kịch bản dữ liệu đến trễ để CHỨNG MINH Event Time hoạt động

Đây là điều **improve.md không đề cập** nhưng lại là bằng chứng thuyết phục nhất: thêm 1 nhánh trong `LabeledTransactionSource` cố tình phát một giao dịch có `timestamp` **lùi lại vài giây so với các giao dịch đã phát trước đó** (giả lập gói tin đến trễ do mạng), rồi chứng minh:
- Với cấu hình hiện tại (`forMonotonousTimestamps` — giả định dữ liệu luôn đến đúng thứ tự) → giao dịch trễ sẽ bị watermark bỏ qua hoặc gây lỗi giả định.
- Đổi sang `forBoundedOutOfOrderness(Duration.ofSeconds(5))` → hệ thống vẫn xử lý đúng giao dịch đến trễ trong biên độ cho phép.

So sánh 2 cấu hình này bằng số liệu (bao nhiêu giao dịch trễ bị bỏ lọt ở mỗi cấu hình) là một biểu đồ/bảng rất mạnh cho phần "Kết quả thực nghiệm".

---

## Giai đoạn 4 — State TTL (chi phí thấp, nên làm)

Thêm vào `open()` của `FraudDetector`:

```java
import org.apache.flink.api.common.state.StateTtlConfig;
import java.time.Duration;

StateTtlConfig ttlConfig = StateTtlConfig
        .newBuilder(Duration.ofMinutes(10))
        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
        .build();

ValueStateDescriptor<Boolean> flagDescriptor = new ValueStateDescriptor<>("flag", Boolean.class);
flagDescriptor.enableTimeToLive(ttlConfig);
flagState = getRuntimeContext().getState(flagDescriptor);
// Lặp lại cho timerState, recentTimestamps, recentAmounts với TTL phù hợp
```

Giải thích trong báo cáo: vì hệ thống mô phỏng xoay vòng qua **200 tài khoản** liên tục, nếu chạy đủ lâu (hàng giờ/ngày thay vì demo vài chục giây), state của các tài khoản không hoạt động sẽ tồn tại vĩnh viễn trong bộ nhớ TaskManager — TTL giải quyết đúng vấn đề này.

---

## Giai đoạn 5 — Unit Test & Stateful Test (điểm cộng phỏng vấn rất lớn)

Đây là phần **hoàn toàn chưa có** (không có `src/test/java` nào trong project) và là phần hay bị hỏi nhất khi phỏng vấn vị trí Data Engineer — vì nó chứng minh bạn hiểu Flink không chỉ ở mức "code chạy được".

```java
package com.tiendat.frauddetection.functions;

import com.tiendat.frauddetection.model.Alert;
import com.tiendat.frauddetection.model.Transaction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class FraudDetectorTest {

    @Test
    void testSmallThenLarge_shouldTriggerAlert() throws Exception {
        FraudDetector detector = new FraudDetector();
        KeyedOneInputStreamOperatorTestHarness<Long, Transaction, Alert> harness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        new KeyedProcessOperator<>(detector),
                        Transaction::getAccountId, org.apache.flink.api.common.typeinfo.Types.LONG);

        harness.open();

        Transaction small = new Transaction(); small.setAccountId(1L); small.setAmount(0.50); small.setTimestamp(1000L);
        Transaction large = new Transaction(); large.setAccountId(1L); large.setAmount(800.00); large.setTimestamp(1300L);

        harness.processElement(small, 1000L);
        harness.processElement(large, 1300L);

        assertThat(harness.extractOutputValues()).hasSize(1);
        assertThat(harness.extractOutputValues().get(0).getRuleId()).isEqualTo("R1");
    }

    @Test
    void testSmallThenLarge_afterTimeout_shouldNotTriggerAlert() throws Exception {
        // Tương tự test trên nhưng khoảng cách timestamp > R1_TIMEOUT
        // -> assert danh sách output RỖNG
    }
}
```

Thêm dependency test vào `pom.xml`:
```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-test-utils</artifactId>
    <version>${flink.version}</version>
    <scope>test</scope>
</dependency>
<dependency>
    <groupId>org.junit.jupiter</groupId>
    <artifactId>junit-jupiter</artifactId>
    <version>5.10.2</version>
    <scope>test</scope>
</dependency>
```

Viết tối thiểu **4 test case** (mỗi rule 1 test: trigger đúng + không trigger sai) là đủ để thể hiện năng lực, không cần phủ 100% coverage.

---

## Giai đoạn 6 (Tùy chọn — chỉ làm nếu còn nhiều thời gian) — Kafka Source/Sink

Chỉ nên làm **sau khi** đã hoàn thành Giai đoạn 1–5, vì đây là hạng mục tốn thời gian nhất và rủi ro dở dang cao nhất (đúng như đã cảnh báo ở lượt trò chuyện trước). Nếu làm, chỉ cần thay **Sink** trước (dễ hơn Source vì không phải lo dựng thêm Kafka Connect/Producer riêng):

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-connector-kafka</artifactId>
    <version>3.2.0-1.19</version>
</dependency>
```

Không cần làm Broadcast State Pattern (rule động qua Kafka) trừ khi bạn thực sự muốn đầu tư — đây là phần phức tạp nhất trong toàn bộ `improve.md` và giá trị tăng thêm cho portfolio cá nhân không tương xứng với thời gian bỏ ra.

---

## Giai đoạn 7 — Dọn dẹp repo trước khi đưa lên GitHub làm portfolio

Đây là bước **rất dễ bị bỏ qua nhưng ảnh hưởng trực tiếp đến ấn tượng đầu tiên** khi nhà tuyển dụng mở repo:

```bash
# Các file/thư mục nên xóa hoặc gitignore trước khi public:
rm -rf .github/modernize        # dấu vết công cụ AI agent, không liên quan đồ án
rm -f note.txt tree.txt improve.txt   # ghi chú làm việc cá nhân, không cần thiết cho reviewer
```

Kiểm tra `.gitignore` đã loại trừ:
```
target/
output/*.csv
dependency-reduced-pom.xml
```
nhưng **giữ lại** `results/` (kèm 1-2 file CSV mẫu + ảnh chụp Web UI) làm bằng chứng trực quan — đây chính là nội dung sẽ lấp vào 2 chỗ `*Note: Please place your screenshot...*` đang còn để trống trong `README.md` hiện tại của bạn.

Cuối cùng, cập nhật `README.md` (đã viết khá tốt sẵn) thêm 1 mục **"Known Limitations"** liệt kê trung thực các giới hạn (file I/O chỉ chạy đúng với parallelism=1, chưa dùng Broadcast State cho dynamic rule, R1/R3 vẫn dùng Processing Time...) — đây là dấu hiệu của tư duy kỹ sư trưởng thành, luôn được đánh giá cao hơn một README "che hết nhược điểm".

---

## Checklist tổng hợp theo thứ tự nên làm

- [ ] Giai đoạn 1: Đổi package, tự viết `Transaction`/`Alert` POJO (thêm field `ruleId`), xóa `flink-walkthrough-common`
- [ ] Giai đoạn 2: Chuyển `AlertSink`/`GroundTruthLogger` sang `RichSinkFunction` mở file 1 lần
- [ ] Giai đoạn 3: Chuyển R2 sang Event-Time Window; thêm kịch bản dữ liệu đến trễ để so sánh 2 `WatermarkStrategy`
- [ ] Giai đoạn 4: Thêm `StateTtlConfig` cho toàn bộ state trong `FraudDetector`
- [ ] Giai đoạn 5: Viết ≥ 4 unit/stateful test bằng `KeyedOneInputStreamOperatorTestHarness`
- [ ] Giai đoạn 6 (tùy chọn): Kafka Sink nếu còn thời gian
- [ ] Giai đoạn 7: Dọn `.github/modernize`, `note.txt`, `tree.txt`; cập nhật `.gitignore`; bổ sung ảnh chụp thật vào `README.md`; thêm mục "Known Limitations"
