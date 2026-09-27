package com.frauddetection.functions;

import com.frauddetection.model.Alert;
import com.frauddetection.model.Transaction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * FraudDetector — KeyedProcessFunction xử lý 2 rule phát hiện gian lận:
 *
 * R1: Small-then-Large — Giao dịch test < 1.00 USD rồi rút > 500 USD trong vòng 1 giây mô phỏng (R1_TIMEOUT)
 * R3: Deviation from Average — Giao dịch > 5× trung bình 10 giao dịch gần nhất
 *
 * Tính năng nâng cao:
 * - Quản trị bộ nhớ State nâng cao với StateTtlConfig (TTL 10 phút):
 *   Tự động thu hồi và dọn dẹp vùng nhớ của các tài khoản không còn hoạt động,
 *   ngăn chặn rò rỉ bộ nhớ (Memory Leak) và lỗi Out-Of-Memory (OOM) khi chạy dài hạn.
 */
public class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {

    private static final long serialVersionUID = 1L;

    // Ngưỡng cấu hình cho các rule
    private static final double SMALL_AMOUNT = 1.00;
    private static final double LARGE_AMOUNT = 500.00;
    private static final long R1_TIMEOUT = 1000L; // 1 giây cho kịch bản demo (thay vì 60s thực tế)
    private static final double DEVIATION_FACTOR = 5.0;
    private static final int HISTORY_SIZE = 10;

    // --- State cho R1: small-then-large ---
    private transient ValueState<Boolean> flagState;
    private transient ValueState<Long> timerState;

    // --- State cho R3: deviation from average ---
    private transient ListState<Double> recentAmounts;

    @Override
    public void open(Configuration parameters) {
        // Cấu hình State TTL (Time-To-Live):
        // 1. Thời gian sống: 10 phút kể từ lần cập nhật cuối cùng.
        // 2. OnCreateAndWrite: gia hạn TTL mỗi khi tạo mới hoặc ghi đè state.
        // 3. NeverReturnExpired: không bao giờ trả về state đã quá hạn sử dụng.
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Duration.ofMinutes(10))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

        // 1. Áp dụng TTL cho cờ đánh dấu giao dịch nhỏ (flagState)
        ValueStateDescriptor<Boolean> flagDescriptor =
                new ValueStateDescriptor<>("flag", Boolean.class);
        flagDescriptor.enableTimeToLive(ttlConfig);
        flagState = getRuntimeContext().getState(flagDescriptor);

        // 2. Áp dụng TTL cho mốc thời gian hẹn giờ (timerState)
        ValueStateDescriptor<Long> timerDescriptor =
                new ValueStateDescriptor<>("timer-state", Long.class);
        timerDescriptor.enableTimeToLive(ttlConfig);
        timerState = getRuntimeContext().getState(timerDescriptor);

        // 3. Áp dụng TTL cho lịch sử số tiền 10 giao dịch gần nhất (recentAmounts)
        ListStateDescriptor<Double> amountsDescriptor =
                new ListStateDescriptor<>("recent-amounts", Double.class);
        amountsDescriptor.enableTimeToLive(ttlConfig);
        recentAmounts = getRuntimeContext().getListState(amountsDescriptor);
    }

    @Override
    public void processElement(Transaction transaction, Context ctx, Collector<Alert> out)
            throws Exception {
        // Áp dụng lần lượt R1 và R3 cho từng giao dịch
        checkR1_SmallThenLarge(transaction, ctx, out);
        checkR3_DeviationFromAverage(transaction, out);
    }

    /**
     * R1 — Small-then-Large: phát hiện giao dịch test thẻ nhỏ (< $1.00)
     * sau đó rút lớn (> $500.00) trong cửa sổ R1_TIMEOUT.
     */
    private void checkR1_SmallThenLarge(Transaction t, Context ctx, Collector<Alert> out)
            throws Exception {
        Boolean lastTransactionWasSmall = flagState.value();

        if (lastTransactionWasSmall != null) {
            if (t.getAmount() > LARGE_AMOUNT) {
                out.collect(new Alert(t.getAccountId(), t.getAmount(), "R1", t.getTimestamp()));
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
     * R3 — Deviation from Average: phát hiện giao dịch vượt quá 5× trung bình 10 giao dịch gần nhất.
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

        // Yêu cầu tối thiểu 3 giao dịch lịch sử để giá trị trung bình mang tính đại diện thống kê
        if (amounts.size() >= 3) {
            double avg = amounts.stream().mapToDouble(Double::doubleValue).average().orElse(0);
            if (avg > 0 && t.getAmount() > avg * DEVIATION_FACTOR) {
                out.collect(new Alert(t.getAccountId(), t.getAmount(), "R3", t.getTimestamp()));
            }
        }

        amounts.add(t.getAmount());
        if (amounts.size() > HISTORY_SIZE) {
            amounts.remove(0);
        }
        recentAmounts.update(amounts);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out)
            throws Exception {
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
