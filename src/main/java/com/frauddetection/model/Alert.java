package com.frauddetection.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * Alert — Domain Model đại diện cho một cảnh báo gian lận được phát hiện.
 *
 * Khác với Alert gốc của flink-walkthrough-common (chỉ có id), model này bổ sung:
 * - amount: số tiền tại thời điểm cảnh báo
 * - ruleId: mã định danh quy tắc phát hiện (R1, R2, R3, CEP_R4,...)
 * - timestamp: mốc thời gian phát sinh cảnh báo
 *
 * Tuân thủ chuẩn Flink POJO để tối ưu hóa serialization trong quá trình luân chuyển qua Flink sink.
 */
public class Alert implements Serializable {

    private static final long serialVersionUID = 1L;

    private long accountId;
    private double amount;
    private String ruleId;
    private long timestamp;

    public Alert() {
        // Bắt buộc: No-argument constructor cho Flink PojoSerializer
    }

    public Alert(long accountId, double amount, String ruleId, long timestamp) {
        this.accountId = accountId;
        this.amount = amount;
        this.ruleId = ruleId;
        this.timestamp = timestamp;
    }

    public long getAccountId() {
        return accountId;
    }

    public void setAccountId(long accountId) {
        this.accountId = accountId;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public String getRuleId() {
        return ruleId;
    }

    public void setRuleId(String ruleId) {
        this.ruleId = ruleId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Alert alert = (Alert) o;
        return accountId == alert.accountId &&
                Double.compare(alert.amount, amount) == 0 &&
                timestamp == alert.timestamp &&
                Objects.equals(ruleId, alert.ruleId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(accountId, amount, ruleId, timestamp);
    }

    @Override
    public String toString() {
        return String.format("Alert{accountId=%d, amount=%.2f, ruleId='%s', timestamp=%d}",
                accountId, amount, ruleId, timestamp);
    }
}
