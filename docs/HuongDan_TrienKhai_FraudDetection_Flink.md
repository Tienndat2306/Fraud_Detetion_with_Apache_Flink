# Tài liệu hướng dẫn triển khai demo: Phát hiện gian lận giao dịch thẻ bằng Apache Flink

**Phạm vi:** tài liệu này hướng dẫn từ A-Z cách triển khai ứng dụng minh họa, dựa trên nguyên tắc đã thống nhất: **mã nguồn walkthrough chính thức của Apache Flink làm nền tảng kỹ thuật**, còn **rules, dataset có nhãn, kịch bản thực nghiệm và phân tích kết quả là phần nhóm tự thiết kế và triển khai**.

---

## 0. Chuẩn bị môi trường

| Công cụ | Phiên bản khuyến nghị | Mục đích |
|---|---|---|
| Java (JDK) | 25 LTS | Bắt buộc để build/chạy ứng dụng |
| Maven | ≥ 3.6 | Build project |
| IDE | IntelliJ IDEA / VS Code + Java Extension Pack | Viết & debug code |
| Docker + Docker Compose | Bản mới nhất | Dựng cluster Flink thật ở bước triển khai cuối |
| Python 3.9+ | — | Viết dataset sinh dữ liệu có nhãn + script tính Precision/Recall |

Kiểm tra nhanh môi trường:
```bash
java -version      # phải ra 25.x
mvn -version
docker --version
```

---

## 1. Khởi tạo project từ archetype chính thức

```bash
mvn archetype:generate \
  -DarchetypeGroupId=org.apache.flink \
  -DarchetypeArtifactId=flink-walkthrough-datastream-java \
  -DarchetypeVersion=1.20.0 \
  -DgroupId=frauddetection \
  -DartifactId=frauddetection \
  -Dversion=0.1 \
  -Dpackage=spendreport \
  -DinteractiveMode=false
```

Cấu trúc project sinh ra:
```
frauddetection/
├── pom.xml
└── src/main/java/spendreport/
    ├── FraudDetectionJob.java   # job chính (KHÔNG cần sửa nhiều)
    └── FraudDetector.java       # class TRỐNG — nơi nhóm viết logic
```

Import project vào IDE dưới dạng Maven project.

---

## 2. Thiết kế bộ Rules (phần đóng góp #1 của nhóm)

Thay vì chỉ giữ 1 rule gốc, nhóm triển khai **4 rule độc lập, có thể kết hợp**, mỗi rule ánh xạ tới một hành vi gian lận thực tế:

| # | Tên rule | Điều kiện kích hoạt | Hành vi gian lận mô phỏng |
|---|---|---|---|
| R1 | Small-then-Large | Giao dịch < 1.00 USD, sau đó giao dịch > 500 USD trong vòng 1 phút | Kẻ gian "test thẻ" bằng giao dịch nhỏ trước khi rút lớn |
| R2 | High Frequency | > 5 giao dịch trong vòng 60 giây trên cùng tài khoản | Thẻ bị dùng tự động (bot) để quét nhiều giao dịch nhỏ |
| R3 | Deviation from Average | Giao dịch > 5 lần giá trị trung bình của 10 giao dịch gần nhất | Giao dịch bất thường so với hành vi tiêu dùng thông thường |
| R4 (CEP) | Multi-step Pattern | Nhỏ → Nhỏ → Lớn, cả 3 trong vòng 2 phút | Mẫu hành vi gian lận phức tạp hơn 1 bước |

### 2.1. Code triển khai R1–R3 bằng KeyedProcessFunction

```java
// FraudDetector.java
package spendreport;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;

import java.util.ArrayList;
import java.util.List;

public class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {

    private static final double SMALL_AMOUNT = 1.00;
    private static final double LARGE_AMOUNT = 500.00;
    private static final long ONE_MINUTE = 60 * 1000;
    private static final int FREQ_THRESHOLD = 5;
    private static final double DEVIATION_FACTOR = 5.0;
    private static final int HISTORY_SIZE = 10;

    // --- State cho R1: small-then-large ---
    private transient ValueState<Boolean> flagState;
    private transient ValueState<Long> timerState;

    // --- State cho R2: high frequency ---
    private transient ListState<Long> recentTimestamps;

    // --- State cho R3: deviation from average ---
    private transient ListState<Double> recentAmounts;

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) {
        flagState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("flag", Boolean.class));
        timerState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("timer-state", Long.class));
        recentTimestamps = getRuntimeContext().getListState(
                new ListStateDescriptor<>("recent-ts", Long.class));
        recentAmounts = getRuntimeContext().getListState(
                new ListStateDescriptor<>("recent-amounts", Double.class));
    }

    @Override
    public void processElement(Transaction transaction, Context ctx, Collector<Alert> out) throws Exception {
        checkR1_SmallThenLarge(transaction, ctx, out);
        checkR2_HighFrequency(transaction, ctx, out);
        checkR3_DeviationFromAverage(transaction, out);
    }

    // R1 — giữ nguyên tinh thần bản gốc, đã tách thành hàm riêng
    private void checkR1_SmallThenLarge(Transaction t, Context ctx, Collector<Alert> out) throws Exception {
        Boolean lastTransactionWasSmall = flagState.value();

        if (lastTransactionWasSmall != null) {
            if (t.getAmount() > LARGE_AMOUNT) {
                Alert alert = new Alert();
                alert.setId(t.getAccountId());
                out.collect(alert); // TODO: gắn thêm nhãn "R1" khi in log để phục vụ phân tích
            }
            cleanUpR1(ctx);
        }

        if (t.getAmount() < SMALL_AMOUNT) {
            flagState.update(true);
            long timer = ctx.timerService().currentProcessingTime() + ONE_MINUTE;
            timerState.update(timer);
            ctx.timerService().registerProcessingTimeTimer(timer);
        }
    }

    // R2 — đếm số giao dịch trong sliding window 60s bằng ListState
    private void checkR2_HighFrequency(Transaction t, Context ctx, Collector<Alert> out) throws Exception {
        long now = ctx.timerService().currentProcessingTime();
        List<Long> timestamps = new ArrayList<>();
        for (Long ts : recentTimestamps.get()) {
            if (now - ts <= ONE_MINUTE) timestamps.add(ts); // chỉ giữ lại các ts trong 60s gần nhất
        }
        timestamps.add(now);
        recentTimestamps.update(timestamps);

        if (timestamps.size() > FREQ_THRESHOLD) {
            Alert alert = new Alert();
            alert.setId(t.getAccountId());
            out.collect(alert); // TODO: gắn nhãn "R2"
        }
    }

    // R3 — so sánh với trung bình động của N giao dịch gần nhất
    private void checkR3_DeviationFromAverage(Transaction t, Collector<Alert> out) throws Exception {
        List<Double> amounts = new ArrayList<>();
        for (Double a : recentAmounts.get()) amounts.add(a);

        if (!amounts.isEmpty()) {
            double avg = amounts.stream().mapToDouble(Double::doubleValue).average().orElse(0);
            if (avg > 0 && t.getAmount() > avg * DEVIATION_FACTOR) {
                Alert alert = new Alert();
                alert.setId(t.getAccountId());
                out.collect(alert); // TODO: gắn nhãn "R3"
            }
        }

        amounts.add(t.getAmount());
        if (amounts.size() > HISTORY_SIZE) amounts.remove(0);
        recentAmounts.update(amounts);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) throws Exception {
        Long timerTimestamp = timerState.value();
        if (timerTimestamp != null && timerTimestamp.equals(timestamp)) {
            cleanUpR1(ctx);
        }
    }

    private void cleanUpR1(Context ctx) throws Exception {
        Long timer = timerState.value();
        if (timer != null) ctx.timerService().deleteProcessingTimeTimer(timer);
        timerState.clear();
        flagState.clear();
    }
}
```

> **Lưu ý quan trọng:** đoạn code trên là khung tham khảo để bạn hiểu cách phối hợp nhiều rule trên cùng một `KeyedProcessFunction`. Khi nộp báo cáo, nhóm nên **tự thêm log/nhãn rule** (ví dụ field `ruleId` trong `Alert`) để bước phân tích ở mục 5 tách được cảnh báo theo từng rule.

### 2.2. R4 — mở rộng bằng Flink CEP (điểm khác biệt kỹ thuật rõ nhất so với bản gốc)

```java
Pattern<Transaction, ?> fraudPattern = Pattern.<Transaction>begin("first")
        .where(new SimpleCondition<Transaction>() {
            @Override
            public boolean filter(Transaction t) { return t.getAmount() < SMALL_AMOUNT; }
        })
        .next("second")
        .where(new SimpleCondition<Transaction>() {
            @Override
            public boolean filter(Transaction t) { return t.getAmount() < SMALL_AMOUNT; }
        })
        .next("third")
        .where(new SimpleCondition<Transaction>() {
            @Override
            public boolean filter(Transaction t) { return t.getAmount() > LARGE_AMOUNT; }
        })
        .within(Time.minutes(2));

PatternStream<Transaction> patternStream = CEP.pattern(
        transactions.keyBy(Transaction::getAccountId), fraudPattern);

DataStream<Alert> r4Alerts = patternStream.select(
        (Map<String, List<Transaction>> pattern) -> {
            Alert alert = new Alert();
            alert.setId(pattern.get("third").get(0).getAccountId());
            return alert; // TODO: gắn nhãn "R4"
        });
```

---

## 3. Thiết kế Dataset có nhãn (phần đóng góp #2 — quan trọng nhất)

Bộ sinh dữ liệu gốc (`TransactionSource`) phát ra giao dịch **hoàn toàn ngẫu nhiên, không có nhãn** → không thể tính Precision/Recall. Nhóm cần viết một `SourceFunction` tùy chỉnh, biết trước và ghi lại nhãn thật (ground truth) của từng giao dịch.

```java
// LabeledTransactionSource.java
public class LabeledTransactionSource implements SourceFunction<Transaction> {

    private volatile boolean running = true;
    private final Random rnd = new Random(42); // seed cố định để tái lập kết quả

    @Override
    public void run(SourceContext<Transaction> ctx) throws Exception {
        long accountId = 1;
        while (running) {
            double roll = rnd.nextDouble();

            if (roll < 0.15) {
                // === Kịch bản GIAN LẬN (15%): small -> large trong 20s ===
                emit(ctx, accountId, 0.50, "FRAUD_SMALL");
                Thread.sleep(500);
                emit(ctx, accountId, 800.00, "FRAUD_LARGE"); // ground truth: label=1
            } else if (roll < 0.25) {
                // === Kịch bản GÂY NHIỄU (10%): small -> large nhưng CÁCH XA thời gian ===
                emit(ctx, accountId, 0.50, "NOISE_SMALL");
                Thread.sleep(90_000); // 90s, vượt khung timer 60s -> không phải gian lận
                emit(ctx, accountId, 800.00, "NOISE_LARGE"); // ground truth: label=0
            } else {
                // === Giao dịch bình thường (75%) ===
                emit(ctx, accountId, 5 + rnd.nextDouble() * 100, "NORMAL"); // label=0
            }
            accountId = (accountId % 50) + 1; // xoay vòng 50 tài khoản giả lập
            Thread.sleep(200);
        }
    }

    private void emit(SourceContext<Transaction> ctx, long accountId, double amount, String tag) {
        Transaction t = new Transaction();
        t.setAccountId(accountId);
        t.setAmount(amount);
        t.setTimestamp(System.currentTimeMillis());
        ctx.collect(t);
        // Ghi log riêng ra file ground-truth.csv: accountId, amount, tag, timestamp, expectedLabel
        GroundTruthLogger.log(accountId, amount, tag, t.getTimestamp());
    }

    @Override
    public void cancel() { running = false; }
}
```

**Ý tưởng cốt lõi:** mỗi giao dịch phát ra đều được ghi đồng thời vào một file `ground-truth.csv` với nhãn thật (`expectedLabel = 1` nếu thuộc kịch bản gian lận thật, `0` nếu không). File này là "đáp án" để đối chiếu với cảnh báo mà hệ thống Flink thực sự sinh ra ở bước 5.

---

## 4. Thiết kế kịch bản thực nghiệm

| Kịch bản | Cách thực hiện | Mục tiêu kiểm chứng |
|---|---|---|
| TN1 — Phát hiện đúng (True Positive) | Chạy `LabeledTransactionSource`, lọc các sự kiện `FRAUD_LARGE` | R1 phải sinh cảnh báo cho toàn bộ các trường hợp này |
| TN2 — Không báo động giả (True Negative) | Lọc các sự kiện `NOISE_LARGE` | R1 **không** được sinh cảnh báo (vì đã quá khung 60s) |
| TN3 — Tần suất bất thường | Tạo thêm nhánh sinh 10 giao dịch/giây cho 1 account_id cố định trong 5 giây | R2 phải kích hoạt |
| TN4 — Độ lệch trung bình | 1 account có lịch sử toàn giao dịch ~10 USD, chèn 1 giao dịch 200 USD | R3 phải kích hoạt |
| TN5 — Chịu tải | Tăng tốc độ phát sinh dữ liệu từ 5 event/s lên 500, 5000 event/s | Đo throughput, độ trễ xử lý qua Flink Web UI |
| TN6 — Khả năng phục hồi | Bật checkpoint (`env.enableCheckpointing(30000)`), kill 1 TaskManager giữa lúc chạy (nếu chạy trên Docker cluster) | Kiểm tra job tự phục hồi từ checkpoint, không mất trạng thái |
| TN7 — So sánh rule đơn vs. kết hợp | Chạy lần lượt: chỉ R1 / R1+R2 / R1+R2+R3+R4 | So sánh Precision/Recall giữa các cấu hình |

---

## 5. Phân tích kết quả — tính Precision / Recall / F1

Sau khi chạy xong mỗi kịch bản, đối chiếu file cảnh báo hệ thống sinh ra (`alerts.csv`, ghi từ `AlertSink`) với file `ground-truth.csv` bằng script Python:

```python
# evaluate.py
import pandas as pd

ground_truth = pd.read_csv("ground-truth.csv")   # accountId, timestamp, expectedLabel
alerts = pd.read_csv("alerts.csv")               # accountId, timestamp, ruleId

# Gộp theo accountId + khung thời gian gần nhất (dung sai 2 giây)
ground_truth["detected"] = 0
for _, alert in alerts.iterrows():
    mask = (
        (ground_truth["accountId"] == alert["accountId"]) &
        (abs(ground_truth["timestamp"] - alert["timestamp"]) < 2000)
    )
    ground_truth.loc[mask, "detected"] = 1

TP = ((ground_truth.expectedLabel == 1) & (ground_truth.detected == 1)).sum()
FN = ((ground_truth.expectedLabel == 1) & (ground_truth.detected == 0)).sum()
FP = ((ground_truth.expectedLabel == 0) & (ground_truth.detected == 1)).sum()
TN = ((ground_truth.expectedLabel == 0) & (ground_truth.detected == 0)).sum()

precision = TP / (TP + FP) if (TP + FP) else 0
recall    = TP / (TP + FN) if (TP + FN) else 0
f1        = 2 * precision * recall / (precision + recall) if (precision + recall) else 0

print(f"Precision={precision:.3f}  Recall={recall:.3f}  F1={f1:.3f}")
print(f"TP={TP} FP={FP} FN={FN} TN={TN}")
```

Chạy script này cho từng cấu hình rule ở kịch bản TN7 để có **bảng so sánh định lượng** — đây là phần "phân tích" thực sự (không chỉ log ra là chạy được), rất phù hợp để đưa vào phần Kết quả thực nghiệm của báo cáo giai đoạn 2.

Ngoài ra, lấy thêm 3 số liệu từ **Flink Web UI** (`localhost:8081`) khi chạy kịch bản TN5/TN6 để đưa vào báo cáo:
- Throughput (records/s) tại các mức tải khác nhau.
- Độ trễ trung bình theo cửa sổ (nếu dùng window) hoặc thời gian phản hồi theo sự kiện.
- Thời gian mỗi lần checkpoint (Checkpoints tab).

---

## 6. Đóng gói & chạy trên cluster thật

```bash
mvn clean package
```

Dựng cluster bằng Docker Compose (file `docker-compose.yml` mẫu):
```yaml
version: "3"
services:
  jobmanager:
    image: flink:1.20.0-java11
    ports: ["8081:8081"]
    command: jobmanager
    environment: [JOB_MANAGER_RPC_ADDRESS=jobmanager]
  taskmanager:
    image: flink:1.20.0-java11
    depends_on: [jobmanager]
    command: taskmanager
    environment: [JOB_MANAGER_RPC_ADDRESS=jobmanager]
    deploy:
      replicas: 2
```

```bash
docker compose up -d
flink run target/frauddetection-0.1.jar
```

Theo dõi tại `http://localhost:8081`.

---

## 7. Checklist trước khi viết báo cáo giai đoạn 2

- [ ] Đã cài đặt đủ 4 rule (R1–R4), có log rõ ruleId cho từng cảnh báo
- [ ] Đã có `LabeledTransactionSource` sinh dữ liệu có ground-truth
- [ ] Đã chạy đủ 7 kịch bản thực nghiệm (TN1–TN7), lưu lại log/số liệu từng lần
- [ ] Đã tính được Precision/Recall/F1 cho từng cấu hình rule
- [ ] Đã chụp số liệu throughput/latency/checkpoint từ Flink Web UI
- [ ] Đã viết rõ trong báo cáo: phần nào tham khảo từ walkthrough gốc, phần nào là đóng góp độc lập của nhóm

---

## 8. Câu khẳng định ranh giới đóng góp (dùng trong báo cáo)

> *"Nhóm sử dụng kiến trúc job và cơ chế State/Timer API tham khảo từ walkthrough Fraud Detection chính thức của Apache Flink (Apache Software Foundation). Bộ rule phát hiện gian lận (R1–R4), bộ dữ liệu thực nghiệm có nhãn (LabeledTransactionSource), các kịch bản kiểm thử (TN1–TN7) và toàn bộ phần phân tích định lượng kết quả (Precision/Recall/F1, throughput, độ trễ phát hiện) là phần thiết kế và triển khai độc lập của nhóm."*
