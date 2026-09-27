package com.frauddetection.sink;

import com.frauddetection.model.Alert;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;

import java.nio.charset.StandardCharsets;
import java.util.Locale;

/**
 * KafkaAlertSink — Nhà máy khởi tạo Apache Flink KafkaSink phân tán.
 *
 * Chịu trách nhiệm:
 * 1. Thiết lập kết nối Kafka Producer tới cụm Kafka Brokers.
 * 2. Tuần tự hóa đối tượng POJO Alert sang định dạng JSON payload.
 * 3. Đẩy thông điệp vào Kafka Topic (mặc định: "fraud-alerts").
 */
public class KafkaAlertSink {

    /**
     * Khởi tạo KafkaSink cho Alert POJO.
     *
     * @param bootstrapServers Địa chỉ Kafka Brokers (ví dụ: "localhost:9092" hoặc "kafka:9092")
     * @param topic            Tên Kafka topic nhận cảnh báo (ví dụ: "fraud-alerts")
     * @return Flink KafkaSink<Alert>
     */
    public static KafkaSink<Alert> createSink(String bootstrapServers, String topic) {
        return KafkaSink.<Alert>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<Alert>builder()
                                .setTopic(topic)
                                .setValueSerializationSchema(new AlertJsonSerializationSchema())
                                .build()
                )
                .build();
    }

    /**
     * Schema tuần tự hóa Alert sang chuỗi JSON chuẩn UTF-8.
     */
    public static class AlertJsonSerializationSchema implements SerializationSchema<Alert> {
        private static final long serialVersionUID = 1L;

        @Override
        public byte[] serialize(Alert alert) {
            String json = String.format(Locale.US,
                    "{\"accountId\":%d,\"amount\":%.2f,\"ruleId\":\"%s\",\"timestamp\":%d}",
                    alert.getAccountId(),
                    alert.getAmount(),
                    alert.getRuleId(),
                    alert.getTimestamp()
            );
            return json.getBytes(StandardCharsets.UTF_8);
        }
    }
}
