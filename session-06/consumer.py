"""Kafka consumer for the Session 6 lab (terminal version of kafka_consumer.ipynb).

Examples (run in the session-06 folder, .venv activated, broker on localhost:9092):
    python consumer.py                               # group group-a, topic user-events, until Ctrl+C
    python consumer.py --group group-a --name C2     # a SECOND consumer in the same group (2nd terminal)
    python consumer.py --group group-b               # a different group: gets ALL messages again
    python consumer.py --from latest                 # a NEW group starts at the end instead of the beginning
    python consumer.py --idle 10                     # stop after 10 s without new messages

Every line shows:   [consumer name] p<partition> @ <offset>  key=...  seq=...
"ASSIGNED" lines show which partitions this consumer got after a rebalance.
"""
import argparse
import json
from collections import Counter

from kafka import ConsumerRebalanceListener, DefaultSerializer, JsonSerializer, KafkaConsumer
from kafka.errors import KafkaError


class PrintAssignment(ConsumerRebalanceListener):
    """Kafka calls these two methods when the group is rebalanced."""

    def __init__(self, name):
        self.name = name

    def on_partitions_revoked(self, revoked):
        if revoked:
            print(f"[{self.name}] REVOKED  {sorted(tp.partition for tp in revoked)}")

    def on_partitions_assigned(self, assigned):
        print(f"[{self.name}] ASSIGNED partitions {sorted(tp.partition for tp in assigned)}")


def main():
    p = argparse.ArgumentParser(description="Read JSON messages from Kafka.")
    p.add_argument("--topic", default="user-events")
    p.add_argument("--group", default="group-a", help="consumer group id (default group-a)")
    p.add_argument("--name", default="C1", help="label printed in front of every line")
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--from", dest="offset_reset", choices=["earliest", "latest"], default="earliest",
                   help="where a NEW group starts (a group that already committed offsets continues there)")
    p.add_argument("--idle", type=float, default=0, help="stop after this many seconds without messages (0 = never)")
    a = p.parse_args()

    try:
        consumer = KafkaConsumer(
            bootstrap_servers=[a.bootstrap],
            group_id=a.group,
            auto_offset_reset=a.offset_reset,
            enable_auto_commit=True,             # offsets are committed in the background (every 5 s)
            key_deserializer=DefaultSerializer(),   # bytes -> str (None stays None)
            value_deserializer=JsonSerializer(),    # bytes -> json.loads(...) -> dict
        )
    except KafkaError as e:   # e.g. KafkaTimeoutError: Unable to bootstrap
        raise SystemExit(f"✗ Cannot reach a broker at {a.bootstrap} ({e}). Is Kafka running? See README.md.")
    consumer.subscribe([a.topic], listener=PrintAssignment(a.name))

    print(f"[{a.name}] group={a.group!r} topic={a.topic!r} – Ctrl+C to stop")
    per_partition = Counter()
    last_seq = {}                 # (run, key) -> last seq seen, to check ordering per key
    idle = 0.0
    try:
        while True:
            batch = consumer.poll(timeout_ms=1000)        # returns {TopicPartition: [records]}
            if not batch:
                idle += 1
                if a.idle and idle >= a.idle:
                    print(f"[{a.name}] no messages for {a.idle:g} s – stopping")
                    break
                continue
            idle = 0.0
            for tp, records in batch.items():
                for r in records:
                    per_partition[r.partition] += 1
                    v = r.value
                    note = ""
                    if isinstance(v, dict) and "seq" in v:
                        k = (v.get("run"), v.get("state"))
                        if k in last_seq and v["seq"] != last_seq[k] + 1:
                            note = f"   ⚠ expected seq {last_seq[k] + 1}"
                        last_seq[k] = v["seq"]
                        body = f"seq={v['seq']:<3} {v['action']}"
                    else:
                        body = json.dumps(v)[:60]
                    print(f"[{a.name}] p{r.partition} @ {r.offset:<4} key={str(r.key):<18} {body}{note}")
    except KeyboardInterrupt:
        print(f"\n[{a.name}] stopping consumer …")
    finally:
        consumer.close()          # leaves the group cleanly → the others get its partitions at once
        print(f"[{a.name}] messages per partition: {dict(sorted(per_partition.items()))}")


if __name__ == "__main__":
    main()
