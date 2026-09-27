package com.frauddetection.functions;

import com.frauddetection.model.Alert;
import com.frauddetection.model.Transaction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * FraudDetectorTest — Bộ kiểm thử trạng thái (Stateful Unit Tests) cho Flink Operator.
 *
 * Sử dụng KeyedOneInputStreamOperatorTestHarness từ flink-test-utils:
 * Giả lập môi trường Flink Runtime độc lập, cho phép can thiệp và kiểm tra chính xác
 * hành vi của Keyed State (ValueState, ListState) và TimerService mà không cần dựng cluster.
 */
class FraudDetectorTest {

    private KeyedOneInputStreamOperatorTestHarness<Long, Transaction, Alert> harness;
    private FraudDetector detector;

    @BeforeEach
    void setUp() throws Exception {
        detector = new FraudDetector();
        // Khởi tạo TestHarness đóng gói KeyedProcessOperator
        harness = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(detector),
                Transaction::getAccountId,
                Types.LONG
        );
        harness.open();
    }

    @Test
    @DisplayName("R1: Giao dịch nhỏ nối tiếp giao dịch lớn trong 1s -> Kích hoạt Alert R1")
    void testSmallThenLarge_withinTimeout_shouldTriggerAlertR1() throws Exception {
        Transaction small = new Transaction(1L, 0.50, 1000L);
        Transaction large = new Transaction(1L, 800.00, 1300L);

        harness.setProcessingTime(1000L);
        harness.processElement(small, 1000L);

        harness.setProcessingTime(1300L);
        harness.processElement(large, 1300L);

        List<Alert> alerts = harness.extractOutputValues();
        assertThat(alerts).hasSize(1);

        Alert alert = alerts.get(0);
        assertThat(alert.getAccountId()).isEqualTo(1L);
        assertThat(alert.getAmount()).isEqualTo(800.00);
        assertThat(alert.getRuleId()).isEqualTo("R1");
    }

    @Test
    @DisplayName("R1: Giao dịch lớn xảy ra sau khi Timer 1s đã hết hạn -> Không kích hoạt Alert (Timer dọn dẹp cờ)")
    void testSmallThenLarge_afterTimeout_shouldNotTriggerAlert() throws Exception {
        Transaction small = new Transaction(1L, 0.50, 1000L);
        Transaction large = new Transaction(1L, 800.00, 2500L);

        harness.setProcessingTime(1000L);
        harness.processElement(small, 1000L);

        // Giả lập đồng hồ hệ thống chạy qua mốc 2100ms (> 1000ms timeout) để kích hoạt onTimer()
        harness.setProcessingTime(2100L);

        // Giao dịch lớn đến muộn
        harness.processElement(large, 2500L);

        List<Alert> alerts = harness.extractOutputValues();
        assertThat(alerts).isEmpty();
    }

    @Test
    @DisplayName("R3: Giao dịch vượt quá 5x trung bình 10 giao dịch gần nhất -> Kích hoạt Alert R3")
    void testDeviationFromAverage_exceedsThreshold_shouldTriggerAlertR3() throws Exception {
        // Gửi chuỗi 10 giao dịch thông thường có giá trị $10.00 (trung bình = $10.00)
        for (int i = 0; i < 10; i++) {
            Transaction normal = new Transaction(2L, 10.00, 1000L + i * 100);
            harness.processElement(normal, normal.getTimestamp());
        }

        // Gửi giao dịch đột biến $200.00 (> 5x * 10.00 = $50.00)
        Transaction spike = new Transaction(2L, 200.00, 2500L);
        harness.processElement(spike, spike.getTimestamp());

        List<Alert> alerts = harness.extractOutputValues();
        assertThat(alerts).hasSize(1);

        Alert alert = alerts.get(0);
        assertThat(alert.getAccountId()).isEqualTo(2L);
        assertThat(alert.getAmount()).isEqualTo(200.00);
        assertThat(alert.getRuleId()).isEqualTo("R3");
    }

    @Test
    @DisplayName("R3: Giao dịch trong ngưỡng an toàn bình thường -> Không kích hoạt Alert")
    void testDeviationFromAverage_normalTransaction_shouldNotTriggerAlert() throws Exception {
        for (int i = 0; i < 10; i++) {
            Transaction normal = new Transaction(3L, 10.00, 1000L + i * 100);
            harness.processElement(normal, normal.getTimestamp());
        }

        // Giao dịch $25.00 (< 5x * 10.00 = $50.00)
        Transaction normalNext = new Transaction(3L, 25.00, 2500L);
        harness.processElement(normalNext, normalNext.getTimestamp());

        List<Alert> alerts = harness.extractOutputValues();
        assertThat(alerts).isEmpty();
    }
}
