"""
kafka_consumer.py — Lắng nghe các cảnh báo gian lận từ Apache Kafka Topic theo thời gian thực.

Cách chạy:
    python scripts/kafka_consumer.py [--topic fraud-alerts] [--bootstrap localhost:9092]

Yêu cầu:
    pip install kafka-python
"""

import argparse
import json
import sys

def main():
    parser = argparse.ArgumentParser(description="Lắng nghe cảnh báo gian lận từ Kafka.")
    parser.add_argument("--bootstrap", default="localhost:9092", help="Địa chỉ Kafka Broker (mặc định: localhost:9092)")
    parser.add_argument("--topic", default="fraud-alerts", help="Tên Kafka Topic (mặc định: fraud-alerts)")
    args = parser.parse_args()

    try:
        from kafka import KafkaConsumer
    except ImportError:
        print("\n[ERROR] Thư viện 'kafka-python' chưa được cài đặt.")
        print("Vui lòng chạy: pip install kafka-python\n")
        sys.exit(1)

    print("=" * 60)
    print("  KAFKA ALERT STREAM CONSUMER")
    print(f"  Broker: {args.bootstrap} | Topic: {args.topic}")
    print("  Đang lắng nghe cảnh báo thời gian thực... (Nhấn Ctrl+C để dừng)")
    print("=" * 60)

    try:
        consumer = KafkaConsumer(
            args.topic,
            bootstrap_servers=[args.bootstrap],
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="fraud-monitor-group",
            value_deserializer=lambda x: json.loads(x.decode("utf-8"))
        )

        alert_count = 0
        for message in consumer:
            alert = message.value
            alert_count += 1
            print(f"[{alert_count:03d}] [KAFKA ALERT] Rule: {alert.get('ruleId', 'N/A')} "
                  f"| Account: {alert.get('accountId', 'N/A')} "
                  f"| Amount: ${alert.get('amount', 0.0):.2f} "
                  f"| Timestamp: {alert.get('timestamp', 'N/A')}")

    except KeyboardInterrupt:
        print("\n\nĐã dừng lắng nghe Kafka.")
    except Exception as e:
        print(f"\n[ERROR] Không thể kết nối tới Kafka ({args.bootstrap}): {e}")
        print("Hãy đảm bảo container Kafka đang chạy: docker compose up -d kafka")

if __name__ == "__main__":
    main()
