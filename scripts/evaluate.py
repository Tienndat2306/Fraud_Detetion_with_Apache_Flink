import pandas as pd
import os
import sys
import io

# Đảm bảo in tiếng Việt trên console Windows không bị UnicodeEncodeError
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except (AttributeError, io.UnsupportedOperation):
        pass


def load_data():
    """Đọc file ground-truth và alerts với tìm kiếm linh hoạt thư mục output."""
    search_paths = [
        os.path.join("output", "ground-truth.csv"),
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", "ground-truth.csv"),
        os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "output", "ground-truth.csv")
    ]

    gt_path = None
    for p in search_paths:
        if os.path.exists(p):
            gt_path = p
            break

    if not gt_path:
        print("ERROR: Không tìm thấy file ground-truth.csv trong thư mục output/")
        print("Hãy chạy FraudDetectionJob trước để sinh dữ liệu.")
        sys.exit(1)

    out_dir = os.path.dirname(gt_path)
    alerts_path = os.path.join(out_dir, "alerts.csv")

    ground_truth = pd.read_csv(gt_path)
    # Lọc bỏ dòng header phụ nếu có
    ground_truth = ground_truth[ground_truth["accountId"].astype(str) != "accountId"].copy()
    ground_truth["accountId"] = pd.to_numeric(ground_truth["accountId"])
    ground_truth["amount"] = pd.to_numeric(ground_truth["amount"])
    ground_truth["timestamp"] = pd.to_numeric(ground_truth["timestamp"])
    ground_truth["expectedLabel"] = pd.to_numeric(ground_truth["expectedLabel"])

    print(f"Loaded ground truth: {len(ground_truth)} records (từ {gt_path})")
    print(f"  - Fraud (label=1): {(ground_truth['expectedLabel'] == 1).sum()}")
    print(f"  - Normal (label=0): {(ground_truth['expectedLabel'] == 0).sum()}")

    if not os.path.exists(alerts_path):
        print(f"\nWARNING: Không tìm thấy file {alerts_path}")
        print("Không có cảnh báo nào được sinh ra.")
        alerts = pd.DataFrame(columns=["accountId", "amount", "ruleId", "timestamp"])
    else:
        alerts = pd.read_csv(alerts_path)
        alerts = alerts[alerts["accountId"].astype(str) != "accountId"].copy()
        alerts["accountId"] = pd.to_numeric(alerts["accountId"])
        alerts["amount"] = pd.to_numeric(alerts["amount"])
        alerts["timestamp"] = pd.to_numeric(alerts["timestamp"])
        print(f"Loaded alerts: {len(alerts)} records (từ {alerts_path})")

    return ground_truth, alerts


def evaluate(ground_truth, alerts, tolerance_ms=500):
    """
    Tính Precision, Recall, F1 bằng cách khớp 1-1 (one-to-one) giữa mỗi
    alert và giao dịch ground-truth GẦN NHẤT còn chưa bị "nhận" bởi alert
    nào khác — tránh việc một alert vô tình đánh dấu "detected" cho nhiều
    giao dịch cùng lúc trong các đợt bùng nổ tần suất cao (FRAUD_HIGHFREQ).

    Lưu ý: tolerance_ms mặc định giảm từ 2000ms xuống 500ms vì
    LabeledTransactionSource phát sinh sự kiện mỗi 200ms — cửa sổ 2 giây
    trước đây bao trùm tới ~10 giao dịch liền kề của cùng 1 tài khoản.
    """
    gt = ground_truth.copy().reset_index(drop=True)
    gt["detected"] = 0

    claimed = set()  # index các dòng ground-truth đã được 1 alert "nhận"

    # Sắp xếp alert theo thời gian để việc khớp lần lượt ổn định, dễ tái lập
    alerts_sorted = alerts.sort_values("timestamp") if len(alerts) else alerts

    for _, alert in alerts_sorted.iterrows():
        candidates = gt[
            (gt["accountId"] == alert["accountId"])
            & (~gt.index.isin(claimed))
            & ((gt["timestamp"] - alert["timestamp"]).abs() < tolerance_ms)
        ]
        if candidates.empty:
            continue  # alert không khớp được giao dịch nào còn trống -> bỏ qua

        # Chọn đúng 1 giao dịch gần thời điểm alert nhất
        nearest_idx = (candidates["timestamp"] - alert["timestamp"]).abs().idxmin()
        gt.loc[nearest_idx, "detected"] = 1
        claimed.add(nearest_idx)

    TP = ((gt["expectedLabel"] == 1) & (gt["detected"] == 1)).sum()
    FN = ((gt["expectedLabel"] == 1) & (gt["detected"] == 0)).sum()
    FP = ((gt["expectedLabel"] == 0) & (gt["detected"] == 1)).sum()
    TN = ((gt["expectedLabel"] == 0) & (gt["detected"] == 0)).sum()

    precision = TP / (TP + FP) if (TP + FP) else 0
    recall = TP / (TP + FN) if (TP + FN) else 0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0

    return TP, FP, FN, TN, precision, recall, f1


def print_results(TP, FP, FN, TN, precision, recall, f1, rule_filter=None):
    """In kết quả đánh giá."""
    header = "Overall Results"
    if rule_filter:
        header = f"Results for Rule: {rule_filter}"

    separator = "=" * 50
    line = "-" * 50

    print(f"\n{separator}")
    print(f"  {header}")
    print(f"{separator}")
    print(f"  True Positives  (TP): {TP}")
    print(f"  False Positives (FP): {FP}")
    print(f"  False Negatives (FN): {FN}")
    print(f"  True Negatives  (TN): {TN}")
    print(f"{line}")
    print(f"  Precision: {precision:.4f}")
    print(f"  Recall:    {recall:.4f}")
    print(f"  F1-Score:  {f1:.4f}")
    print(f"{separator}")


def main():
    print("\n" + "=" * 60)
    print("  FRAUD DETECTION EVALUATION")
    print("  Đánh giá hiệu quả phát hiện gian lận")
    print("=" * 60 + "\n")

    ground_truth, alerts = load_data()

    # 1. Đánh giá tổng thể (dùng chung 1 pool "claimed" cho toàn bộ alert)
    TP, FP, FN, TN, precision, recall, f1 = evaluate(ground_truth, alerts)
    print_results(TP, FP, FN, TN, precision, recall, f1)

    # 2. Đánh giá theo từng rule
    #    Lưu ý: mỗi rule được đánh giá ĐỘC LẬP trên toàn bộ ground-truth
    #    (không dùng chung "claimed" với rule khác và không dùng chung
    #    với bước 1), vì mục tiêu là so sánh hiệu năng TỪNG rule riêng lẻ
    #    NẾU nó là rule duy nhất đang chạy — không phải hiệu năng cộng dồn.
    if len(alerts) > 0 and "ruleId" in alerts.columns:
        for rule_id in sorted(alerts["ruleId"].unique()):
            rule_alerts = alerts[alerts["ruleId"] == rule_id]
            TP_r, FP_r, FN_r, TN_r, p_r, r_r, f1_r = evaluate(
                ground_truth, rule_alerts
            )
            print_results(TP_r, FP_r, FN_r, TN_r, p_r, r_r, f1_r, rule_filter=rule_id)

    # 3. Thống kê phân bổ
    print(f"\n{'=' * 50}")
    print("  Distribution Summary")
    print(f"{'=' * 50}")
    if "tag" in ground_truth.columns:
        print("\n  Ground Truth Tags:")
        tag_counts = ground_truth["tag"].value_counts()
        for tag, count in tag_counts.items():
            print(f"    {tag}: {count}")

    if len(alerts) > 0 and "ruleId" in alerts.columns:
        print("\n  Alert Rules:")
        rule_counts = alerts["ruleId"].value_counts()
        for rule, count in rule_counts.items():
            print(f"    {rule}: {count}")

    print()


if __name__ == "__main__":
    main()