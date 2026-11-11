# Block 1 – Kafka broker and command-line tools  (≈ 60 min)

**Goal:** after this block you can create a topic with several partitions, explain what `--describe` shows,
predict which partition a keyed message lands in, read consumer **lag**, and find your messages
as bytes in a log segment on disk.

You need a running broker (README.md → *Before the session*). Work in pairs: one types, one predicts – swap after each part.

**Write your predictions down before you press Enter.** The exam asks *why*, not *which command*.

---

## 0. Choose your route (2 min)

Every step shows two variants. Use **one** of them for the whole block.

| | Route A – Docker (most of you) | Route B – native Kafka (`get_kafka.py`) |
|---|---|---|
| Where the CLI tools run | **inside** the container: `docker exec -it broker /opt/kafka/bin/<tool>.sh …` | on your laptop: `<tool>.sh …` |
| Where the data lives | `/tmp/kafka-logs/` inside the container | `kafka-data/` next to the Kafka folder |
| Works in | PowerShell, macOS/Linux terminal | macOS/Linux/WSL terminal |

**Route B only** – once per terminal, in the folder where `get_kafka.py` installed Kafka
(`session-06/`, or the folder you gave with `--dest`):
```bash
export PATH="$PWD/kafka_2.13-4.4.0/bin:$PATH"
export KDATA="$PWD/kafka-data"
kafka-topics.sh --version            # → 4.4.0
```

All commands are written on **one line**, so they also work in PowerShell (which does not understand `\` line breaks).
The message `Picked up JAVA_TOOL_OPTIONS …` or a hint about *KIP-848* can be ignored.

---

## 1. Which topics exist? (3 min)

| Route A | Route B |
|---|---|
| `docker exec -it broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list` | `kafka-topics.sh --bootstrap-server localhost:9092 --list` |

On a fresh broker the list is empty or shows only `__consumer_offsets` (an internal topic – what might it store? Answer in part 6).

`--bootstrap-server localhost:9092` = "the first broker I talk to". From it, the client learns about the whole cluster.

---

## 2. Create a topic with 3 partitions (8 min)

✏️ **Prediction 1:** we have **one** broker. What happens if you ask for replication factor 2?

| Route A | Route B |
|---|---|
| `docker exec -it broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic cli-demo --partitions 3 --replication-factor 2` | `kafka-topics.sh --bootstrap-server localhost:9092 --create --topic cli-demo --partitions 3 --replication-factor 2` |

Now with replication factor 1:

| Route A | Route B |
|---|---|
| `docker exec -it broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic cli-demo --partitions 3 --replication-factor 1` | `kafka-topics.sh --bootstrap-server localhost:9092 --create --topic cli-demo --partitions 3 --replication-factor 1` |

Describe it:

| Route A | Route B |
|---|---|
| `docker exec -it broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic cli-demo` | `kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic cli-demo` |

Expected (your TopicId differs):
```
Topic: cli-demo  TopicId: …  PartitionCount: 3  ReplicationFactor: 1  Configs: …segment.bytes=1048576
    Topic: cli-demo  Partition: 0  Leader: 1  Replicas: 1  Isr: 1  …
    Topic: cli-demo  Partition: 1  Leader: 1  Replicas: 1  Isr: 1  …
    Topic: cli-demo  Partition: 2  Leader: 1  Replicas: 1  Isr: 1  …
```

| Column | Meaning |
|---|---|
| `Leader` | id of the broker that handles reads and writes for this partition |
| `Replicas` | all brokers that keep a copy (here only broker 1) |
| `Isr` | **in-sync replicas**: the copies that are fully up to date. Only these can become the new leader without losing data |

✏️ **Question:** in a cluster with 3 brokers and replication factor 3, what would `Replicas` and `Isr` look like? What changes in `Isr` if one broker crashes?

---

## 3. Produce messages with a key (10 min)

A Kafka message is a **key–value pair**. The producer picks the partition from the key:
`partition = hash(key) mod number_of_partitions`. No key → the producer spreads messages over the partitions.

Start the console producer. `parse.key=true` + `key.separator=:` means: the text before the first `:` is the key.

| Route A | Route B |
|---|---|
| `docker exec -it broker /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic cli-demo --reader-property parse.key=true --reader-property key.separator=:` | `kafka-console-producer.sh --bootstrap-server localhost:9092 --topic cli-demo --reader-property parse.key=true --reader-property key.separator=:` |

At the `>` prompt type these lines, one per Enter, then **Ctrl+C**:
```
Tirol:first order
Wien:hello
Tirol:second order
Salzburg:hi
Tirol:third order
Wien:bye
```

✏️ **Prediction 2:** will the three `Tirol` messages end up in one partition or in different ones? And the two `Wien` messages?

> Older Kafka versions and many tutorials write `--property` instead of `--reader-property`. In Kafka 4.4 `--property` still works but is deprecated.

---

## 4. Consume from the beginning (8 min)

| Route A | Route B |
|---|---|
| `docker exec -it broker /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic cli-demo --from-beginning --formatter-property print.key=true --formatter-property print.partition=true --formatter-property print.offset=true` | `kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic cli-demo --from-beginning --formatter-property print.key=true --formatter-property print.partition=true --formatter-property print.offset=true` |

Stop with **Ctrl+C** after the messages appear. Example (your partition numbers may differ, the pattern will not):
```
Partition:0  Offset:0  Tirol     first order
Partition:0  Offset:1  Tirol     second order
Partition:0  Offset:2  Tirol     third order
Partition:1  Offset:0  Wien      hello
Partition:1  Offset:1  Salzburg  hi
Partition:1  Offset:2  Wien      bye
```

✏️ Check your predictions and answer:
1. Is the output in the order you typed? Which order **is** guaranteed?
2. Every partition starts at offset 0. What is an offset, and is it unique in the topic or only in a partition?
3. Two different keys (`Wien`, `Salzburg`) can share a partition. Why is that not a problem?
4. Run the command without `--from-beginning`. Why do you see nothing now? (Slides: by default a consumer only reads what is produced after it connected.)

---

## 5. Consumer groups and lag (12 min)

A consumer **group** remembers how far it has read: its **committed offset** per partition, stored in the topic `__consumer_offsets`.
**Lag = log-end offset − committed offset** = messages written but not yet processed by this group.

**5a.** Read everything once as group `cli-group`, then **Ctrl+C**:

| Route A | Route B |
|---|---|
| `docker exec -it broker /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic cli-demo --group cli-group --from-beginning` | `kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic cli-demo --group cli-group --from-beginning` |

**5b.** While **no** consumer runs, produce five more messages (producer command from part 3):
```
Tirol:fourth
Wien:again
Vorarlberg:new
Tirol:fifth
Steiermark:x
```

**5c.** ✏️ **Prediction 3:** what is the total LAG of `cli-group` now? Then check:

| Route A | Route B |
|---|---|
| `docker exec -it broker /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group cli-group` | `kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group cli-group` |

Expected shape (numbers per partition depend on the keys):
```
Consumer group 'cli-group' has no active members.
GROUP      TOPIC     PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID ...
cli-group  cli-demo  0          3               6               3    -
cli-group  cli-demo  1          3               4               1    -
cli-group  cli-demo  2          0               1               1    -
```

**5d.** Start the consumer of 5a again – **with** `--group cli-group`, but this time **without** `--from-beginning`.
Which messages do you get? Describe the group again: what is the LAG now?

**5e.** List all groups (`kafka-consumer-groups.sh … --list`, Route A with the `docker exec … /opt/kafka/bin/` prefix).
You will see groups like `console-consumer-12345`: a console consumer **without** `--group` invents a random group.
That is why part 4 always started again at the beginning.

---

## 6. Your messages on disk (12 min)

A partition is a folder; inside are **segment** files. The `.log` file is the append-only log: messages are only
ever added at the end. (`__consumer_offsets-0 … -49` are the 50 partitions of the internal offsets topic.)

**Route A – Docker**
```bash
docker exec -it broker bash -lc "ls /tmp/kafka-logs"
docker exec -it broker bash -lc "ls -la /tmp/kafka-logs/cli-demo-0"
docker exec -it broker /opt/kafka/bin/kafka-dump-log.sh --files /tmp/kafka-logs/cli-demo-0/00000000000000000000.log --print-data-log
docker exec -it broker bash -lc "hexdump -C /tmp/kafka-logs/cli-demo-0/00000000000000000000.log | head -20"
docker cp broker:/tmp/kafka-logs/cli-demo-0/00000000000000000000.log ./segment.log
```

**Route B – native**
```bash
ls "$KDATA"
ls -la "$KDATA/cli-demo-0"
kafka-dump-log.sh --files "$KDATA/cli-demo-0/00000000000000000000.log" --print-data-log
hexdump -C "$KDATA/cli-demo-0/00000000000000000000.log" | head -20
```
(If `Tirol` landed in another partition for you, use that partition's folder.)

**No `hexdump`** (Windows)? After `docker cp`, run this in Python (a notebook cell or `python` in the terminal):
```python
data = open("segment.log", "rb").read()[:320]
for i in range(0, len(data), 16):
    chunk = data[i:i + 16]
    text = "".join(chr(b) if 32 <= b < 127 else "." for b in chunk)
    print(f"{i:08x}  {chunk.hex(' '):<47}  |{text}|")
```

What you see in `kafka-dump-log` (shortened):
```
baseOffset: 0 lastOffset: 2 count: 3 ... magic: 2 compresscodec: none crc: ... isvalid: true
| offset: 0 CreateTime: ... keySize: 5 valueSize: 11 ... key: Tirol payload: first order
| offset: 1 CreateTime: ... keySize: 5 valueSize: 12 ... key: Tirol payload: second order
```
(How many messages share one batch depends on how fast you typed – the console producer waits a moment to batch.)
and in the hex dump, among binary header bytes:
```
...  0a 54 69 72 6f 6c 16 66 69 72 73 74 20 6f 72  |..Tirol.first or|
```

✏️ Answer:
1. Messages are grouped in **batches** (`baseOffset … lastOffset … count`). Who creates the batches?
2. Key and value are just **bytes** for Kafka. Who turns them back into text or JSON? (→ Block 2: deserializer)
3. The folder also has `.index` and `.timeindex` files. What could they be good for when a consumer asks for offset 4 711?
4. With 1 MB segments (our course setting), what happens to the folder after a few thousand messages?

---

## 7. More partitions later? (5 min)

✏️ **Prediction 4:** we change the topic from 3 to 4 partitions. Does `Tirol` still go to the same partition?

| Route A | Route B |
|---|---|
| `docker exec -it broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --alter --topic cli-demo --partitions 4` | `kafka-topics.sh --bootstrap-server localhost:9092 --alter --topic cli-demo --partitions 4` |

Produce one more message each for `Tirol`, `Wien`, `Salzburg` (part 3), consume with `--from-beginning` (part 4) and compare
the partition numbers. Then try `--partitions 2`. What happens, and why can Kafka not do that?

(Tested on Kafka 4.4.0: after the change, `Tirol` and `Wien` moved to other partitions – `hash(key) mod 4` ≠ `hash(key) mod 3`.)

Clean up (optional): `kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic cli-demo` (Route A with the prefix).

---

## 8. Exam-style questions

1. *(2 P)* Define **offset**. Is an offset unique within a topic or within a partition?
2. *(3 P)* A topic has 3 partitions. Messages with the key `Tirol` always arrive in the order they were produced, but
   messages without a key do not. Explain why.
3. *(3 P)* `kafka-consumer-groups --describe` shows `CURRENT-OFFSET 120`, `LOG-END-OFFSET 200` for one partition.
   What is the lag, and give two possible reasons why it is growing.
4. *(4 P)* Your laptop has one broker. Why does `--replication-factor 2` fail? In production with 3 brokers, what do
   `Leader`, `Replicas` and `Isr` mean, and what happens when the leader broker fails?
5. *(4 P)* A colleague increases a topic from 3 to 6 partitions to get more throughput. Their downstream job relies on
   all events of one customer being processed in order. What can go wrong, and what would you do?
