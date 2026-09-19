package spendreport;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

/**
 * Fraud Detection Job — Main entry point.
 *
 * Triển khai hệ thống phát hiện gian lận giao dịch thẻ sử dụng Apache Flink.
 * Kết hợp 4 rule phát hiện:
 *   R1: Small-then-Large (test thẻ trước khi rút lớn)
 *   R2: High Frequency (bot quét giao dịch)
 *   R3: Deviation from Average (giao dịch bất thường)
 *   R4: Multi-step Pattern via CEP (Nhỏ → Nhỏ → Lớn)
 */
public class FraudDetectionJob {

    private static final double SMALL_AMOUNT = 1.00;
    private static final double LARGE_AMOUNT = 500.00;

    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws Exception {
        // 1. Khởi tạo môi trường thực thi
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Bật checkpoint mỗi 30 giây để hỗ trợ khả năng phục hồi (TN6)
        env.enableCheckpointing(30000);

        // 2. Nguồn dữ liệu có nhãn kèm WatermarkStrategy cho CEP và Event-time
        DataStream<Transaction> transactions = env
                .addSource(new LabeledTransactionSource())
                .name("labeled-transactions")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Transaction>forMonotonousTimestamps()
                                .withTimestampAssigner((t, ts) -> t.getTimestamp())
                );

        // 3. Xử lý R1, R2, R3 qua KeyedProcessFunction
        DataStream<Alert> r123Alerts = transactions
                .keyBy(Transaction::getAccountId)
                .process(new FraudDetector())
                .name("fraud-detector-r1-r2-r3");

        // 4. Xử lý R4 qua Flink CEP — Multi-step Pattern: Nhỏ → Nhỏ → Lớn trong 2 phút
        Pattern<Transaction, ?> fraudPattern = Pattern.<Transaction>begin("first")
                .where(new SimpleCondition<>() {
                    @Override
                    public boolean filter(Transaction t) {
                        return t.getAmount() < SMALL_AMOUNT;
                    }
                })
                .next("second")
                .where(new SimpleCondition<>() {
                    @Override
                    public boolean filter(Transaction t) {
                        return t.getAmount() < SMALL_AMOUNT;
                    }
                })
                .next("third")
                .where(new SimpleCondition<>() {
                    @Override
                    public boolean filter(Transaction t) {
                        return t.getAmount() > LARGE_AMOUNT;
                    }
                })
                .within(Time.minutes(2));

        PatternStream<Transaction> patternStream = CEP.pattern(
                transactions.keyBy(Transaction::getAccountId), fraudPattern);

        DataStream<Alert> r4Alerts = patternStream.select(
                (Map<String, List<Transaction>> pattern) -> {
                    Transaction matched = pattern.get("third").get(0);
                    Alert alert = new Alert();
                    alert.setId(matched.getAccountId());
                    System.out.println("[ALERT R4 - CEP Multi-step] Account: " + matched.getAccountId()
                            + " | Amount: " + matched.getAmount());
                    // Ghi vào file alerts
                    AlertSink.logAlert(matched.getAccountId(), matched.getAmount(), "R4",
                            matched.getTimestamp());
                    return alert;
                });

        // 5. Union tất cả alerts và đưa vào sink
        DataStream<Alert> allAlerts = r123Alerts.union(r4Alerts);

        allAlerts.addSink(new AlertSinkFunction())
                .name("alert-sink");

        // 6. Thực thi job
        env.execute("Fraud Detection with 4 Rules");
    }
}
