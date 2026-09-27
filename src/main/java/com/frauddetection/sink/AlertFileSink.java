package com.frauddetection.sink;

import com.frauddetection.model.Alert;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;

/**
 * AlertFileSink — Flink RichSinkFunction chịu trách nhiệm xuất cảnh báo.
 *
 * Tối ưu hóa so với bản mẫu:
 * 1. Khởi tạo BufferedWriter MỘT LẦN DUY NHẤT trong open(), không mở/đóng file liên tục trên mỗi alert.
 * 2. Tách biệt hoàn toàn việc xuất dữ liệu (sink) khỏi logic nghiệp vụ (FraudDetector, CEP).
 * 3. Xuất đồng thời ra console và file output/alerts.csv phục vụ kịch bản đo đạc evaluate.py.
 *
 * Lưu ý: Hoạt động tối ưu với parallelism = 1. Đối với cluster nhiều worker, chuẩn sản xuất
 * sẽ sử dụng Kafka Sink hoặc FileSink dạng phân tán.
 */
@SuppressWarnings("deprecation")
public class AlertFileSink extends RichSinkFunction<Alert> {

    private static final long serialVersionUID = 1L;
    private static final String OUTPUT_DIR = "output";
    private static final String FILE_PATH = OUTPUT_DIR + "/alerts.csv";

    private transient BufferedWriter writer;

    @Override
    public synchronized void open(Configuration parameters) throws Exception {
        Path outputPath = Paths.get(OUTPUT_DIR);
        if (!Files.exists(outputPath)) {
            Files.createDirectories(outputPath);
        }

        // Mở file và ghi header đúng 1 lần duy nhất khi subtask khởi động
        writer = new BufferedWriter(new FileWriter(FILE_PATH, false));
        writer.write("accountId,amount,ruleId,timestamp");
        writer.newLine();
        writer.flush();
    }

    @Override
    public synchronized void invoke(Alert alert, Context context) throws Exception {
        // 1. In thông báo ra console
        System.out.printf(Locale.US,
                ">>> [ALERT %s] Account: %d | Amount: $%.2f | Timestamp: %d%n",
                alert.getRuleId(), alert.getAccountId(), alert.getAmount(), alert.getTimestamp());

        // 2. Ghi vào file alerts.csv
        if (writer != null) {
            writer.write(String.format(Locale.US, "%d,%.2f,%s,%d",
                    alert.getAccountId(), alert.getAmount(), alert.getRuleId(), alert.getTimestamp()));
            writer.newLine();
            writer.flush();
        }
    }

    @Override
    public synchronized void close() throws Exception {
        if (writer != null) {
            writer.flush();
            writer.close();
            writer = null;
        }
    }
}
