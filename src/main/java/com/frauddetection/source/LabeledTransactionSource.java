package com.frauddetection.source;

import com.frauddetection.model.Transaction;
import com.frauddetection.sink.GroundTruthLogger;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * LabeledTransactionSource — nguồn dữ liệu giao dịch giả lập có gán nhãn thực tế (ground truth).
 *
 * Phân bổ dữ liệu giao dịch:
 *   - 15% Kịch bản GIAN LẬN R1 (small → large trong 300ms < R1_TIMEOUT) → expectedLabel = 1
 *   - 10% Kịch bản GÂY NHIỄU (small → large cách nhau 1500ms > R1_TIMEOUT) → expectedLabel = 0
 *   - 10% Kịch bản CEP R4 (small → small → large trong 2 phút) → expectedLabel = 1
 *   - 5%  Kịch bản HIGH FREQUENCY R2 (10 giao dịch nhanh trong 300ms) → expectedLabel = 1
 *   - 5%  Kịch bản DEVIATION R3 (10 txn bình thường → 1 giao dịch đột biến) → expectedLabel = 1
 *   - 5%  Kịch bản GIAO DỊCH ĐẾN TRỄ (LATE ARRIVAL - 2000ms do trễ mạng) → expectedLabel = 0
 *   - 50% Giao dịch tiêu dùng bình thường → expectedLabel = 0
 */
@SuppressWarnings("deprecation")
public class LabeledTransactionSource implements SourceFunction<Transaction> {

    private static final long serialVersionUID = 1L;
    private volatile boolean running = true;
    private final Random rnd = new Random(42); // Seed cố định để kết quả thực nghiệm có thể tái lập

    @Override
    public void run(SourceContext<Transaction> ctx) throws Exception {
        long accountId = 1;
        int txCount = 0;
        final int MAX_TRANSACTIONS = 2000; // Giới hạn số lượng giao dịch cho phiên chạy

        // Khởi tạo file ground-truth.csv
        GroundTruthLogger.init();

        try {
            while (running && txCount < MAX_TRANSACTIONS) {
                double roll = rnd.nextDouble();

                if (roll < 0.15) {
                    // === Kịch bản GIAN LẬN R1 (15%): small -> large trong 300ms (< R1_TIMEOUT 1000ms) ===
                    emit(ctx, accountId, 0.50, "FRAUD_SMALL");
                    txCount++;
                    Thread.sleep(300);
                    emit(ctx, accountId, 800.00, "FRAUD_LARGE");
                    txCount++;

                } else if (roll < 0.25) {
                    // === Kịch bản GÂY NHIỄU (10%): small -> large cách xa nhau (> R1_TIMEOUT) ===
                    emit(ctx, accountId, 0.50, "NOISE_SMALL");
                    txCount++;
                    Thread.sleep(1500); // 1500ms > R1_TIMEOUT để timer xóa cờ flag
                    emit(ctx, accountId, 800.00, "NOISE_LARGE");
                    txCount++;

                } else if (roll < 0.35) {
                    // === Kịch bản CEP R4 (10%): Multi-step Pattern Nhỏ → Nhỏ → Lớn ===
                    emit(ctx, accountId, 0.40, "FRAUD_R4_STEP1");
                    txCount++;
                    Thread.sleep(200);
                    emit(ctx, accountId, 0.60, "FRAUD_R4_STEP2");
                    txCount++;
                    Thread.sleep(200);
                    emit(ctx, accountId, 900.00, "FRAUD_R4_LARGE");
                    txCount++;

                } else if (roll < 0.40) {
                    // === Kịch bản HIGH FREQUENCY R2 (5%): 10 giao dịch dồn dập (300ms) ===
                    for (int i = 0; i < 10; i++) {
                        emit(ctx, accountId, 5.0 + rnd.nextDouble() * 10, "FRAUD_HIGHFREQ");
                        txCount++;
                        Thread.sleep(30);
                    }

                } else if (roll < 0.45) {
                    // === Kịch bản DEVIATION R3 (5%): Giao dịch đột biến sau chuỗi giao dịch nhỏ ===
                    for (int i = 0; i < 10; i++) {
                        emit(ctx, accountId, 8.0 + rnd.nextDouble() * 4, "NORMAL");
                        txCount++;
                        Thread.sleep(50);
                    }
                    emit(ctx, accountId, 200.00, "FRAUD_DEVIATION");
                    txCount++;

                } else if (roll < 0.50) {
                    // === Kịch bản DỮ LIỆU ĐẾN TRỄ (5%): Giả lập mạng chậm, timestamp lùi lại 2000ms ===
                    long currentTs = System.currentTimeMillis();
                    long delayedTs = currentTs - 2000; // Trễ 2 giây so với thời gian máy
                    Transaction lateTx = new Transaction(accountId, 15.0 + rnd.nextDouble() * 30, delayedTs);
                    ctx.collectWithTimestamp(lateTx, lateTx.getTimestamp());
                    GroundTruthLogger.log(accountId, lateTx.getAmount(), "LATE_NORMAL", delayedTs);
                    txCount++;

                } else {
                    // === Giao dịch bình thường (50%) ===
                    double amount = 5 + rnd.nextDouble() * 100;
                    emit(ctx, accountId, amount, "NORMAL");
                    txCount++;
                }

                accountId = (accountId % 200) + 1; // Luân chuyển qua 200 tài khoản
                Thread.sleep(100);
            }
        } finally {
            GroundTruthLogger.close();
        }

        System.out.println("\n=== LabeledTransactionSource finished: " + txCount
                + " transactions emitted ===");
        System.out.println("Ground truth saved to: output/ground-truth.csv");
        System.out.println("Alerts saved to: output/alerts.csv");
    }

    /**
     * Phát ra một đối tượng Transaction POJO và ghi nhận vào ground truth.
     */
    private void emit(SourceContext<Transaction> ctx, long accountId, double amount, String tag) {
        long currentTs = System.currentTimeMillis();
        Transaction t = new Transaction(accountId, amount, currentTs);
        ctx.collectWithTimestamp(t, t.getTimestamp());

        // Ghi nhận ground truth
        GroundTruthLogger.log(accountId, amount, tag, currentTs);
    }

    @Override
    public void cancel() {
        running = false;
        GroundTruthLogger.close();
    }
}
