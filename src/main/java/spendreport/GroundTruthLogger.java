package spendreport;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * GroundTruthLogger — ghi ground truth (nhãn thật) cho từng giao dịch ra file CSV.
 *
 * File output: output/ground-truth.csv
 * Format: accountId,amount,tag,timestamp,expectedLabel
 *
 * expectedLabel = 1 nếu tag bắt đầu bằng "FRAUD", ngược lại = 0.
 * File này là "đáp án" để đối chiếu với alerts khi tính Precision/Recall.
 */
public class GroundTruthLogger {

    private static final String OUTPUT_DIR = "output";
    private static final String FILE_PATH = OUTPUT_DIR + "/ground-truth.csv";

    private GroundTruthLogger() {
        // Private constructor for utility class
    }

    /**
     * Ghi một dòng ground truth vào file CSV.
     */
    public static synchronized void log(long accountId, double amount, String tag, long timestamp) {
        try {
            // Tạo thư mục output nếu chưa tồn tại
            Path outputPath = Paths.get(OUTPUT_DIR);
            if (!Files.exists(outputPath)) {
                Files.createDirectories(outputPath);
            }

            java.io.File file = new java.io.File(FILE_PATH);
            boolean writeHeader = !file.exists() || file.length() == 0;

            try (PrintWriter writer = new PrintWriter(new FileWriter(FILE_PATH, true))) {
                if (writeHeader) {
                    writer.println("accountId,amount,tag,timestamp,expectedLabel");
                }
                int expectedLabel = tag.startsWith("FRAUD") ? 1 : 0;
                writer.printf(java.util.Locale.US, "%d,%.2f,%s,%d,%d%n", accountId, amount, tag, timestamp, expectedLabel);
            }
        } catch (IOException e) {
            System.err.println("[GroundTruthLogger] Error writing to " + FILE_PATH + ": " + e.getMessage());
        }
    }
}
