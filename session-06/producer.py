"""Kafka producer for the Session 6 lab (terminal version of kafka_producer.ipynb).

Examples (run in the session-06 folder, .venv activated, broker on localhost:9092):
    python producer.py                          # keyed events to topic user-events, 1 per second, until Ctrl+C
    python producer.py --count 30 --interval 0.2
    python producer.py --no-key                 # same events, but WITHOUT a key
    python producer.py --data users --topic registered_user   # the original lab: fake users, no key

Every line shows where the message went:   key -> partition @ offset
"""
import argparse
import time

from kafka import DefaultSerializer, JsonSerializer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError

from kafka_data import KeyedEvents, get_registered_user


def ensure_topic(bootstrap, topic, partitions):
    """Create the topic if it does not exist yet (safe to run many times)."""
    admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="session06-admin")
    try:
        admin.create_topics([NewTopic(name=topic, num_partitions=partitions, replication_factor=1)])
        print(f"created topic {topic!r} with {partitions} partitions")
    except TopicAlreadyExistsError:
        pass                                   # fine – it is already there
    finally:
        admin.close()


def main():
    p = argparse.ArgumentParser(description="Send JSON messages to Kafka.")
    p.add_argument("--topic", default="user-events")
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--count", type=int, default=0, help="number of messages (0 = until Ctrl+C)")
    p.add_argument("--interval", type=float, default=1.0, help="seconds between messages")
    p.add_argument("--no-key", action="store_true", help="send without a key")
    p.add_argument("--data", choices=["events", "users"], default="events",
                   help="events = keyed state events (default), users = original fake users (never keyed)")
    p.add_argument("--partitions", type=int, default=3,
                   help="partitions if the topic has to be created (default 3; an existing topic is not changed)")
    a = p.parse_args()

    try:
        ensure_topic(a.bootstrap, a.topic, a.partitions)
        producer = KafkaProducer(
            bootstrap_servers=[a.bootstrap],
            key_serializer=DefaultSerializer(),     # key "Tirol" -> UTF-8 bytes (None stays None)
            value_serializer=JsonSerializer(),      # dict -> json.dumps(...) -> UTF-8 bytes
            acks="all",         # the broker confirms only after the write is complete
            retries=5,          # retry on temporary errors
            linger_ms=10,       # wait up to 10 ms to batch messages
        )
    except KafkaError as e:   # e.g. KafkaTimeoutError: Unable to bootstrap
        raise SystemExit(f"✗ Cannot reach a broker at {a.bootstrap} ({e}). Is Kafka running? See README.md.")

    events = KeyedEvents()
    use_key = a.data == "events" and not a.no_key
    print(f"producing to {a.topic!r} ({'keyed' if use_key else 'NO key'}) – Ctrl+C to stop")
    sent = 0
    try:
        while a.count == 0 or sent < a.count:
            if a.data == "users":
                key, value = None, get_registered_user()
            else:
                key, value = events.next()
                if not use_key:
                    key = None
            meta = producer.send(a.topic, key=key, value=value).get(timeout=10)  # wait → errors show up now
            sent += 1
            label = f"seq={value['seq']:<3}" if "seq" in value else value["name"]
            print(f"key={str(key):<18} {label}  -> partition {meta.partition} @ offset {meta.offset}")
            time.sleep(a.interval)
    except KeyboardInterrupt:
        print("\nstopping producer …")
    except KafkaError as e:
        print(f"Kafka error: {e!r}")
    finally:
        producer.flush()
        producer.close()
        print(f"sent {sent} messages")


if __name__ == "__main__":
    main()
