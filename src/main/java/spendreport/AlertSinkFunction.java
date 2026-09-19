package spendreport;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.walkthrough.common.entity.Alert;

/**
 * AlertSinkFunction — Flink SinkFunction để in alert ra console.
 *
 * Việc ghi file CSV được xử lý bởi AlertSink.logAlert() gọi trực tiếp
 * trong FraudDetector và FraudDetectionJob (cho R4).
 */
@SuppressWarnings("deprecation")
public class AlertSinkFunction implements SinkFunction<Alert> {

    private static final long serialVersionUID = 1L;

    @Override
    public void invoke(Alert alert, Context context) {
        System.out.println(">>> ALERT: Account " + alert.getId() + " triggered fraud detection!");
    }
}
