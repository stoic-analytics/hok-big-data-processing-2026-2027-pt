# Block 1 – Kafka CLI · model answers (NOT for the student repo)

All outputs below were observed on **Kafka 4.4.0, native KRaft single node** (Route B) on 10.11.2026.
The Docker route uses the same Kafka build, so the outputs should match – but it was **not** run (no Docker in my sandbox).

## Predictions

| # | Question | Answer / observed |
|---|---|---|
| 1 | RF 2 with one broker? | Fails: `InvalidReplicationFactorException: Unable to replicate the partition 2 time(s): The target replication factor of 2 cannot be reached because only 1 broker(s) are registered …`. You cannot have more replicas than brokers (slide "Data Replication"). |
| 2 | Three `Tirol` messages – one partition? | Yes, all in one partition (observed: partition 0), and the two `Wien` messages also share one (observed: partition 1, together with `Salzburg`). `partition = murmur2(key) mod 3`. |
| 3 | Lag after 5 new messages? | Total LAG = 5 (observed per partition 3 / 1 / 1 for the given keys). |
| 4 | After 3 → 4 partitions, does `Tirol` stay? | Not guaranteed. Observed: `Tirol` 0 → 1, `Wien` 1 → 3, `Salzburg` stayed in 1. `--partitions 2` fails: `The topic cli-demo currently has 4 partition(s); 2 would not be an increase.` |

Observed partition mapping with 3 partitions (same for the Java CLI and kafka-python, both use murmur2):
Tirol, Kaernten, Burgenland, Steiermark, Niederoesterreich → 0 · Wien, Salzburg, Oberoesterreich → 1 · Vorarlberg → 2.
Good for checking students' screens: if their mapping differs, they have a different partition count.

## Part 2 question – 3 brokers, RF 3
`Replicas: 1,2,3` (order = preferred leader first), `Isr: 1,2,3`. If broker 2 crashes it drops out of the ISR
(`Isr: 1,3`); if it was the leader, a new leader is elected **from the ISR** (in Kafka 4 by the KRaft controller).

## Part 4
1. No – the console consumer prints partition by partition. Guaranteed is only the order **within a partition**.
2. Offset = position of a message in a partition (0, 1, 2, …), assigned by the broker on append. Unique **per partition** only;
   (topic, partition, offset) identifies a message.
3. A partition is not a key-value store: it is a log of many keys. Order per key is still kept because one key never
   spreads over partitions.
4. Without `--from-beginning` a consumer in a new group starts at the end (`auto.offset.reset=latest` default) and
   waits for new messages. (Slide: "By default consumers will only consume data produced after they connected".)

## Part 5
- 5c observed:
  ```
  Consumer group 'cli-group' has no active members.
  GROUP      TOPIC     PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
  cli-group  cli-demo  0          3               6               3
  cli-group  cli-demo  1          3               4               1
  cli-group  cli-demo  2          0               1               1
  ```
- 5d: only the 5 new messages; afterwards LAG 0 everywhere. `auto.offset.reset` only matters for a group **without** committed offsets.
- 5e: `console-consumer-<random>` groups appear because a console consumer without `--group` creates one. `__consumer_offsets`
  stores the committed offsets of all groups (answer to part 1).

## Part 6
1. The **producer** client (it collects messages for `linger.ms` / up to `batch.size` per partition and sends one batch).
   With piped input I saw `count: 3` in one batch; typed by hand often one message per batch.
2. The consumer's **deserializer**. Kafka never parses the payload.
3. `.index` maps offsets → byte position in the `.log`, `.timeindex` maps timestamps → offsets; so the broker can jump
   to offset 4 711 without scanning the file. (The index files are pre-allocated at 10 MB, that is why `ls -la` shows 10 485 760 bytes.)
4. When the active segment reaches 1 MB, Kafka closes it and starts a new one named after its first offset
   (`00000000000000004711.log`). Retention deletes whole old segments (default 7 days, `log.retention.hours=168`).

Observed `kafka-dump-log` (piped input):
```
baseOffset: 0 lastOffset: 2 count: 3 baseSequence: 0 lastSequence: 2 producerId: 3 ... magic: 2 compresscodec: none crc: 3506848845 isvalid: true
| offset: 0 CreateTime: 1794350206038 keySize: 5 valueSize: 11 sequence: 0 headerKeys: [] key: Tirol payload: first order
| offset: 1 CreateTime: 1794350206047 keySize: 5 valueSize: 12 sequence: 1 headerKeys: [] key: Tirol payload: second order
```
Observed hex dump line: `00000040  00 0a 54 69 72 6f 6c 16 66 69 72 73 74 20 6f 72  |..Tirol.first or|`
(`0a` = varint length 5 of the key, `16` = varint length 11 of the value – zig-zag encoded. Only if someone asks.)

## Exam-style questions (Block 1, section 8)

**1 (2 P)** Offset = sequential position of a record in **one partition**, assigned by the broker (1 P). Unique only within
the partition; the same offset exists in every partition (1 P).

**2 (3 P)** Same key → same hash → same partition (1 P); within a partition Kafka appends and reads strictly in order (1 P).
Unkeyed messages are spread over several partitions; consumers read partitions independently, so there is no order across
partitions (1 P).

**3 (3 P)** Lag = 200 − 120 = 80 messages (1 P). Reasons (1 P each, max 2): consumer is too slow / processing per message
expensive; consumer stopped or crashed; producer rate increased; too few consumers (or too few partitions to add consumers);
rebalances interrupt consumption.

**4 (4 P)** RF 2 needs 2 brokers to place two copies; one broker → error (1 P). Leader = broker handling reads/writes for
the partition; Replicas = all brokers holding a copy; Isr = replicas fully caught up with the leader (2 P).
Leader fails → the controller elects a new leader from the ISR, clients switch to it, no committed data is lost (1 P).

**5 (4 P)** `hash(key) mod 6` ≠ `hash(key) mod 3`: after the change, new events of a customer can go to a different partition
than the old ones (2 P). Events still sitting in the old partition (lag!) may be processed *after* newer ones in the new
partition → order per customer broken during the transition (1 P). Fix: plan enough partitions from the start, or drain
consumers (lag 0) before increasing, or create a new topic with more partitions and migrate (1 P).
