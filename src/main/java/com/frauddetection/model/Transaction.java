package com.frauddetection.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * Transaction — Domain Model đại diện cho một giao dịch thẻ.
 *
 * Tuân thủ chuẩn Apache Flink POJO:
 * 1. Class là public.
 * 2. Có default constructor không tham số (no-arg constructor).
 * 3. Tất cả các trường là non-final, có getter và setter theo quy ước JavaBeans.
 * 4. Hỗ trợ Flink PojoTypeInfo và PojoSerializer tối ưu nhất cho hiệu năng stream.
 */
public class Transaction implements Serializable {

    private static final long serialVersionUID = 1L;

    private long accountId;
    private double amount;
    private long timestamp;

    public Transaction() {
        // Bắt buộc: No-argument constructor cho Flink PojoSerializer
    }

    public Transaction(long accountId, double amount, long timestamp) {
        this.accountId = accountId;
        this.amount = amount;
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
        Transaction that = (Transaction) o;
        return accountId == that.accountId &&
                Double.compare(that.amount, amount) == 0 &&
                timestamp == that.timestamp;
    }

    @Override
    public int hashCode() {
        return Objects.hash(accountId, amount, timestamp);
    }

    @Override
    public String toString() {
        return String.format("Transaction{accountId=%d, amount=%.2f, timestamp=%d}",
                accountId, amount, timestamp);
    }
}
