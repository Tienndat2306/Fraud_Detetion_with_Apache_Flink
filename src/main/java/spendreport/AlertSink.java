package spendreport;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * AlertSink — ghi các cảnh báo (alerts) mà hệ thống phát hiện ra file CSV.
 *
 * File output: output/alerts.csv
 * Format: accountId,amount,ruleId,timestamp
 *
 * File này sẽ được đối chiếu với ground-truth.csv để tính Precision/Recall/F1.
 */
public class AlertSink {

    private static final String OUTPUT_DIR = "output";
    private static final String FILE_PATH = OUTPUT_DIR + "/alerts.csv";

    private AlertSink() {
        // Private constructor for utility class
    }

    /**
     * Ghi một dòng alert vào file CSV.
     */
    public static synchronized void logAlert(long accountId, double amount, String ruleId,
                                              long timestamp) {
        try {
            Path outputPath = Paths.get(OUTPUT_DIR);
            if (!Files.exists(outputPath)) {
                Files.createDirectories(outputPath);
            }

            java.io.File file = new java.io.File(FILE_PATH);
            boolean writeHeader = !file.exists() || file.length() == 0;

            try (PrintWriter writer = new PrintWriter(new FileWriter(FILE_PATH, true))) {
                if (writeHeader) {
                    writer.println("accountId,amount,ruleId,timestamp");
                }
                writer.printf(java.util.Locale.US, "%d,%.2f,%s,%d%n", accountId, amount, ruleId, timestamp);
            }
        } catch (IOException e) {
            System.err.println("[AlertSink] Error writing to " + FILE_PATH + ": " + e.getMessage());
        }
    }
}
