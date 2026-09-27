package com.frauddetection.sink;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;

/**
 * GroundTruthLogger — ghi ground truth (nhãn thực tế) cho từng giao dịch ra file CSV.
 *
 * File output: output/ground-truth.csv
 * Format: accountId,amount,tag,timestamp,expectedLabel
 *
 * expectedLabel = 1 nếu tag bắt đầu bằng "FRAUD", ngược lại = 0.
 * File này đóng vai trò ground truth để đối chiếu với alerts.csv khi tính Precision / Recall / F1.
 */
public class GroundTruthLogger {

    private static final String OUTPUT_DIR = "output";
    private static final String FILE_PATH = OUTPUT_DIR + "/ground-truth.csv";
    private static BufferedWriter writer;

    private GroundTruthLogger() {
        // Private constructor for utility class
    }

    /**
     * Khởi tạo file ground-truth.csv và ghi header một lần duy nhất.
     */
    public static synchronized void init() {
        try {
            Path outputPath = Paths.get(OUTPUT_DIR);
            if (!Files.exists(outputPath)) {
                Files.createDirectories(outputPath);
            }
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException ignored) {}
            }
            writer = new BufferedWriter(new FileWriter(FILE_PATH, false));
            writer.write("accountId,amount,tag,timestamp,expectedLabel");
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            System.err.println("[GroundTruthLogger] Error initializing " + FILE_PATH + ": " + e.getMessage());
        }
    }

    /**
     * Ghi một dòng ground truth vào file CSV.
     */
    public static synchronized void log(long accountId, double amount, String tag, long timestamp) {
        try {
            if (writer == null) {
                init();
            }
            int expectedLabel = tag.startsWith("FRAUD") ? 1 : 0;
            writer.write(String.format(Locale.US, "%d,%.2f,%s,%d,%d",
                    accountId, amount, tag, timestamp, expectedLabel));
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            System.err.println("[GroundTruthLogger] Error writing to " + FILE_PATH + ": " + e.getMessage());
        }
    }

    /**
     * Đóng writer khi quá trình phát giao dịch hoàn tất.
     */
    public static synchronized void close() {
        if (writer != null) {
            try {
                writer.flush();
                writer.close();
            } catch (IOException e) {
                System.err.println("[GroundTruthLogger] Error closing " + FILE_PATH + ": " + e.getMessage());
            } finally {
                writer = null;
            }
        }
    }
}
