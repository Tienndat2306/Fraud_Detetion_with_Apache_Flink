package spendreport;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.walkthrough.common.entity.Transaction;

import java.util.Random;

/**
 * LabeledTransactionSource — nguồn dữ liệu giao dịch có nhãn (ground truth).
 *
 * Thay thế TransactionSource mặc định của walkthrough.
 * Mỗi giao dịch phát ra đều được ghi đồng thời vào ground-truth.csv
 * với nhãn thật (expectedLabel) để phục vụ tính Precision/Recall.
 *
 * Phân bổ dữ liệu:
 *   - 15% kịch bản GIAN LẬN R1 (small → large trong 300ms < R1_TIMEOUT) → label=1
 *   - 10% kịch bản GÂY NHIỄU (small → large cách xa 1500ms > R1_TIMEOUT) → label=0
 *   - 10% kịch bản CEP R4 (small → small → large) → label=1
 *   - 5%  kịch bản HIGH FREQUENCY R2 (10 giao dịch nhanh trong 300ms) → label=1
 *   - 5%  kịch bản DEVIATION R3 (10 txn bình thường → 1 giao dịch đột biến) → label=1
 *   - 55% giao dịch tiêu dùng bình thường → label=0
 */
@SuppressWarnings("deprecation")
public class LabeledTransactionSource implements SourceFunction<Transaction> {

    private static final long serialVersionUID = 1L;
    private volatile boolean running = true;
    private final Random rnd = new Random(42); // seed cố định để tái lập kết quả

    @Override
    public void run(SourceContext<Transaction> ctx) throws Exception {
        long accountId = 1;
        int txCount = 0;
        final int MAX_TRANSACTIONS = 2000; // Giới hạn số giao dịch cho demo

        while (running && txCount < MAX_TRANSACTIONS) {
            double roll = rnd.nextDouble();

            if (roll < 0.15) {
                // === Kịch bản GIAN LẬN R1 (15%): small -> large trong 300ms (< R1_TIMEOUT 1000ms) ===
                emit(ctx, accountId, 0.50, "FRAUD_SMALL");
                txCount++;
                Thread.sleep(300); // 300ms nằm trọn trong cửa sổ R1_TIMEOUT
                emit(ctx, accountId, 800.00, "FRAUD_LARGE"); // ground truth: label=1
                txCount++;

            } else if (roll < 0.25) {
                // === Kịch bản GÂY NHIỄU (10%): small -> large nhưng CÁCH XA (> R1_TIMEOUT 1000ms) ===
                emit(ctx, accountId, 0.50, "NOISE_SMALL");
                txCount++;
                // Nghỉ 1500ms > R1_TIMEOUT (1000ms) để timer dọn dẹp cờ flagState
                Thread.sleep(1500);
                emit(ctx, accountId, 800.00, "NOISE_LARGE"); // ground truth: label=0 (không báo động giả)
                txCount++;

            } else if (roll < 0.35) {
                // === Kịch bản CEP R4 (10%): Multi-step Pattern Nhỏ → Nhỏ → Lớn ===
                emit(ctx, accountId, 0.40, "FRAUD_R4_STEP1");
                txCount++;
                Thread.sleep(200);
                emit(ctx, accountId, 0.60, "FRAUD_R4_STEP2");
                txCount++;
                Thread.sleep(200);
                emit(ctx, accountId, 900.00, "FRAUD_R4_LARGE"); // ground truth: label=1
                txCount++;

            } else if (roll < 0.40) {
                // === Kịch bản HIGH FREQUENCY R2 (5%): 10 giao dịch nhanh liên tiếp (300ms) ===
                for (int i = 0; i < 10; i++) {
                    emit(ctx, accountId, 5.0 + rnd.nextDouble() * 10, "FRAUD_HIGHFREQ");
                    txCount++;
                    Thread.sleep(30); // 10 giao dịch hoàn tất trong 300ms
                }

            } else if (roll < 0.45) {
                // === Kịch bản DEVIATION R3 (5%): giao dịch đột biến ===
                // Trước đó phát 10 giao dịch bình thường ~8-12 USD
                for (int i = 0; i < 10; i++) {
                    emit(ctx, accountId, 8.0 + rnd.nextDouble() * 4, "NORMAL");
                    txCount++;
                    Thread.sleep(50);
                }
                // Rồi 1 giao dịch đột biến 200 USD (>5x trung bình ~10)
                emit(ctx, accountId, 200.00, "FRAUD_DEVIATION");
                txCount++;

            } else {
                // === Giao dịch bình thường (55%) ===
                double amount = 5 + rnd.nextDouble() * 100;
                emit(ctx, accountId, amount, "NORMAL");
                txCount++;
            }

            accountId = (accountId % 200) + 1; // xoay vòng 200 tài khoản giả lập để tránh dồn tần suất R2
            Thread.sleep(100); // Khoảng cách giữa các lượt giao dịch
        }

        System.out.println("\n=== LabeledTransactionSource finished: " + txCount
                + " transactions emitted ===");
        System.out.println("Ground truth saved to: output/ground-truth.csv");
        System.out.println("Alerts saved to: output/alerts.csv");
    }

    /**
     * Phát ra 1 giao dịch và đồng thời ghi vào ground-truth.csv.
     */
    private void emit(SourceContext<Transaction> ctx, long accountId, double amount, String tag) {
        Transaction t = new Transaction();
        t.setAccountId(accountId);
        t.setAmount(amount);
        t.setTimestamp(System.currentTimeMillis());
        ctx.collectWithTimestamp(t, t.getTimestamp());

        // Ghi ground truth
        GroundTruthLogger.log(accountId, amount, tag, t.getTimestamp());
    }

    @Override
    public void cancel() {
        running = false;
    }
}
