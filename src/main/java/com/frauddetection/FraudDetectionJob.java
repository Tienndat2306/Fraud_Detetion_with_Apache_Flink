package com.frauddetection;

import com.frauddetection.functions.FraudDetector;
import com.frauddetection.model.Alert;
import com.frauddetection.model.Transaction;
import com.frauddetection.sink.AlertFileSink;
import com.frauddetection.sink.KafkaAlertSink;
import com.frauddetection.source.LabeledTransactionSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * FraudDetectionJob — Pipeline Entry Point.
 *
 * Triển khai hệ thống phát hiện gian lận giao dịch tài chính thời gian thực trên Apache Flink.
 * Kết hợp 4 quy tắc phát hiện:
 *   - R1: Small-then-Large (Test thẻ trước khi rút số tiền lớn) — KeyedProcessFunction
 *   - R2: High Frequency (Bot quét giao dịch tần suất cao) — Tumbling Event-Time Window (Event Time)
 *   - R3: Deviation from Average (Giao dịch lệch chuẩn lịch sử chi tiêu) — KeyedProcessFunction
 *   - R4: Multi-step CEP Pattern (Chuỗi hành vi: Nhỏ → Nhỏ → Lớn) — Flink CEP
 *
 * Tích hợp kiến trúc Dual-Sink:
 *   - Sink 1: AlertFileSink (ghi file CSV phục vụ đo đạc Precision/Recall bằng evaluate.py).
 *   - Sink 2: KafkaAlertSink (đẩy Alert POJO dưới dạng JSON phân tán vào Apache Kafka).
 */
public class FraudDetectionJob {

    private static final double SMALL_AMOUNT = 1.00;
    private static final double LARGE_AMOUNT = 500.00;
    private static final int FREQ_THRESHOLD = 5;

    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws Exception {
        // 1. Khởi tạo môi trường Flink Execution
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Đảm bảo tính nhất quán và phục hồi lỗi với Checkpointing mỗi 30 giây
        env.enableCheckpointing(30000);

        // 2. Nguồn dữ liệu giả lập kèm WatermarkStrategy xử lý Out-Of-Order lên tới 5 giây
        DataStream<Transaction> transactions = env
                .addSource(new LabeledTransactionSource())
                .name("labeled-transactions-source")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((t, ts) -> t.getTimestamp())
                );

        // 3. Nhánh 1: Xử lý quy tắc R1 (Small-then-Large) và R3 (Deviation) qua KeyedProcessFunction
        DataStream<Alert> r13Alerts = transactions
                .keyBy(Transaction::getAccountId)
                .process(new FraudDetector())
                .name("fraud-detector-r1-r3");

        // 4. Nhánh 2: Xử lý quy tắc R2 (High Frequency) qua Tumbling Event-Time Window 10 giây
        DataStream<Alert> r2Alerts = transactions
                .keyBy(Transaction::getAccountId)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
                .apply(new WindowFunction<Transaction, Alert, Long, TimeWindow>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public void apply(Long key, TimeWindow window, Iterable<Transaction> input, Collector<Alert> out) {
                        int count = 0;
                        Transaction last = null;
                        for (Transaction t : input) {
                            count++;
                            last = t;
                        }
                        if (count > FREQ_THRESHOLD && last != null) {
                            out.collect(new Alert(key, last.getAmount(), "R2", last.getTimestamp()));
                        }
                    }
                })
                .name("fraud-detector-r2-event-time-window");

        // 5. Nhánh 3: Xử lý quy tắc R4 qua Flink CEP: Nhỏ (<$1.00) → Nhỏ (<$1.00) → Lớn (>$500.00) trong 2 phút
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
                    return new Alert(matched.getAccountId(), matched.getAmount(), "R4", matched.getTimestamp());
                });

        // 6. Union luồng kết quả cảnh báo từ cả 3 nhánh
        DataStream<Alert> allAlerts = r13Alerts.union(r2Alerts, r4Alerts);

        // 6.1. Sink 1: Ghi file CSV phục vụ kiểm nghiệm & script evaluate.py
        allAlerts.addSink(new AlertFileSink())
                .name("alert-file-sink")
                .setParallelism(1);

        // 6.2. Sink 2 (Giai đoạn 6): Đẩy Alert sang Kafka Topic phân tán
        // Kiểm tra cờ kích hoạt Kafka: hỗ trợ tham số dòng lệnh (--kafka / --kafka-brokers)
        // hoặc System property (-Dkafka.enabled=true / -Dkafka.brokers=localhost:9092)
        boolean kafkaEnabled = false;
        String kafkaBrokers = "localhost:9092";
        String kafkaTopic = "fraud-alerts";

        for (int i = 0; i < args.length; i++) {
            if ("--kafka".equals(args[i])) {
                kafkaEnabled = true;
            } else if ("--kafka-brokers".equals(args[i]) && i + 1 < args.length) {
                kafkaBrokers = args[++i];
                kafkaEnabled = true;
            } else if ("--kafka-topic".equals(args[i]) && i + 1 < args.length) {
                kafkaTopic = args[++i];
            }
        }
        if (System.getProperty("kafka.enabled") != null) {
            kafkaEnabled = Boolean.parseBoolean(System.getProperty("kafka.enabled"));
        }
        if (System.getProperty("kafka.brokers") != null) {
            kafkaBrokers = System.getProperty("kafka.brokers");
        }
        if (System.getProperty("kafka.topic") != null) {
            kafkaTopic = System.getProperty("kafka.topic");
        }

        if (kafkaEnabled) {
            System.out.printf(">>> [Kafka Sink Enabled] Streaming alerts to Kafka: %s (Topic: %s)%n",
                    kafkaBrokers, kafkaTopic);
            allAlerts.sinkTo(KafkaAlertSink.createSink(kafkaBrokers, kafkaTopic))
                    .name("alert-kafka-sink");
        }

        // 7. Kích hoạt Flink DAG
        env.execute("Fraud Detection Job (Clean Architecture & Dual-Sink)");
    }
}
