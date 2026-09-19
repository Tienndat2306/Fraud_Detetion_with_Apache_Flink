package spendreport;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;

import java.util.ArrayList;
import java.util.List;

/**
 * FraudDetector — KeyedProcessFunction xử lý 3 rule phát hiện gian lận.
 *
 * R1: Small-then-Large — Giao dịch < 1.00 USD rồi > 500 USD trong vòng 1 phút
 * R2: High Frequency  — > 5 giao dịch trong vòng 60 giây trên cùng tài khoản
 * R3: Deviation from Average — Giao dịch > 5× trung bình 10 giao dịch gần nhất
 */
public class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {

    private static final long serialVersionUID = 1L;

    // Ngưỡng cấu hình cho các rule (được tối ưu cho mô phỏng dòng sự kiện)
    private static final double SMALL_AMOUNT = 1.00;
    private static final double LARGE_AMOUNT = 500.00;
    private static final long R1_TIMEOUT = 1000L; // 1 giây timeout cho mô phỏng demo (thay vì 60s)
    private static final long R2_WINDOW = 10 * 1000L; // Cửa sổ 10 giây cho phát hiện tần suất cao
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
    public void open(Configuration parameters) {
        // Khởi tạo các state descriptor
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
    public void processElement(Transaction transaction, Context ctx, Collector<Alert> out)
            throws Exception {
        // Áp dụng lần lượt 3 rule cho mỗi giao dịch
        checkR1_SmallThenLarge(transaction, ctx, out);
        checkR2_HighFrequency(transaction, ctx, out);
        checkR3_DeviationFromAverage(transaction, out);
    }

    /**
     * R1 — Small-then-Large: phát hiện giao dịch "test thẻ" nhỏ
     * sau đó rút lớn trong vòng R1_TIMEOUT (1 giây mô phỏng).
     */
    private void checkR1_SmallThenLarge(Transaction t, Context ctx, Collector<Alert> out)
            throws Exception {
        Boolean lastTransactionWasSmall = flagState.value();

        if (lastTransactionWasSmall != null) {
            if (t.getAmount() > LARGE_AMOUNT) {
                Alert alert = new Alert();
                alert.setId(t.getAccountId());
                out.collect(alert);
                System.out.println("[ALERT R1 - Small-then-Large] Account: " + t.getAccountId()
                        + " | Amount: " + t.getAmount());
                AlertSink.logAlert(t.getAccountId(), t.getAmount(), "R1",
                        t.getTimestamp());
            }
            cleanUpR1(ctx);
        }

        if (t.getAmount() < SMALL_AMOUNT) {
            flagState.update(true);
            long timer = ctx.timerService().currentProcessingTime() + R1_TIMEOUT;
            timerState.update(timer);
            ctx.timerService().registerProcessingTimeTimer(timer);
        }
    }

    /**
     * R2 — High Frequency: phát hiện > 5 giao dịch trong R2_WINDOW (10 giây)
     * trên cùng một tài khoản (dấu hiệu bot).
     */
    private void checkR2_HighFrequency(Transaction t, Context ctx, Collector<Alert> out)
            throws Exception {
        long now = ctx.timerService().currentProcessingTime();
        List<Long> timestamps = new ArrayList<>();
        Iterable<Long> currentTs = recentTimestamps.get();
        if (currentTs != null) {
            for (Long ts : currentTs) {
                if (now - ts <= R2_WINDOW) {
                    timestamps.add(ts); // chỉ giữ lại các ts trong R2_WINDOW gần nhất
                }
            }
        }
        timestamps.add(now);
        recentTimestamps.update(timestamps);

        if (timestamps.size() > FREQ_THRESHOLD) {
            Alert alert = new Alert();
            alert.setId(t.getAccountId());
            out.collect(alert);
            System.out.println("[ALERT R2 - High Frequency] Account: " + t.getAccountId()
                    + " | Txn count: " + timestamps.size());
            AlertSink.logAlert(t.getAccountId(), t.getAmount(), "R2",
                    t.getTimestamp());
        }
    }

    /**
     * R3 — Deviation from Average: phát hiện giao dịch có giá trị
     * vượt quá 5 lần trung bình của 10 giao dịch gần nhất.
     */
    private void checkR3_DeviationFromAverage(Transaction t, Collector<Alert> out)
            throws Exception {
        List<Double> amounts = new ArrayList<>();
        Iterable<Double> currentAmounts = recentAmounts.get();
        if (currentAmounts != null) {
            for (Double a : currentAmounts) {
                amounts.add(a);
            }
        }

        if (!amounts.isEmpty()) {
            double avg = amounts.stream().mapToDouble(Double::doubleValue).average().orElse(0);
            if (avg > 0 && t.getAmount() > avg * DEVIATION_FACTOR) {
                Alert alert = new Alert();
                alert.setId(t.getAccountId());
                out.collect(alert);
                System.out.println("[ALERT R3 - Deviation] Account: " + t.getAccountId()
                        + " | Amount: " + t.getAmount() + " | Avg: " + String.format(java.util.Locale.US, "%.2f", avg));
                AlertSink.logAlert(t.getAccountId(), t.getAmount(), "R3",
                        t.getTimestamp());
            }
        }

        // Cập nhật lịch sử giao dịch (giữ tối đa HISTORY_SIZE)
        amounts.add(t.getAmount());
        if (amounts.size() > HISTORY_SIZE) {
            amounts.remove(0);
        }
        recentAmounts.update(amounts);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out)
            throws Exception {
        // Timer hết hạn cho R1 — xóa trạng thái flag
        Long timerTimestamp = timerState.value();
        if (timerTimestamp != null && timerTimestamp.equals(timestamp)) {
            cleanUpR1(ctx);
        }
    }

    /**
     * Dọn dẹp state R1: xóa flag và hủy timer.
     */
    private void cleanUpR1(Context ctx) throws Exception {
        Long timer = timerState.value();
        if (timer != null) {
            ctx.timerService().deleteProcessingTimeTimer(timer);
        }
        timerState.clear();
        flagState.clear();
    }
}
