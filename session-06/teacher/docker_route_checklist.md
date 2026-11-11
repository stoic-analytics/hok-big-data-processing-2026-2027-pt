# Session 6 – Docker route test checklist (tonight, on the Mac)

**Goal:** confirm Route A (Docker) from `01_kafka_cli.md` and the Python clients work end to end before 09:00 tomorrow.
**Time:** about 10 minutes plus the image download (~240 MB, plus ~220 MB if you test the UI).
**Status:** the Docker route has **not** been tested yet; the sandbox has no Docker. The expected outputs below come from
the native Kafka 4.4.0 test and from normal Kafka behaviour. Tick each box, and note anything that differs.

Work from the repo folder on your Mac. Use terminal tab **T1** for the broker and CLI, **T2/T3** for the consumers and
**T4** for the producer.

```bash
cd <your-repo>/session-06
```

---

### Step 1 – Docker Desktop is running (30 s)

```bash
docker version --format '{{.Server.Version}}'
docker compose version
```

- [ ] **Expected:** a server version number and `Docker Compose version v2.x`.
- **If it fails:** "Cannot connect to the Docker daemon" → start Docker Desktop and wait for the whale icon to settle.

### Step 2 – Pull and start the broker (1–2 min)

```bash
docker pull apache/kafka:4.4.0
docker compose up -d
docker compose ps
docker compose logs broker | grep -i "Kafka Server started"
```

- [ ] **Expected:** `ps` shows a container named `broker`, state `running`, port `0.0.0.0:9092->9092/tcp`.
- [ ] **Expected:** the log line contains `Kafka Server started` (it can take 5–15 s; repeat the grep if it is empty).
- **If it fails:** "port is already allocated" → something else uses 9092 (an old native broker?): `lsof -i :9092`, stop it,
  then `docker compose up -d` again. Container exits right away → `docker compose logs broker | tail -40` and look at the
  first `ERROR` line.

### Step 3 – Port reachable from the Mac (10 s)

```bash
nc -vz localhost 9092
```

- [ ] **Expected:** `Connection to localhost port 9092 [tcp/*] succeeded!`

### Step 4 – Topics: the intended failure, then the real topic (1 min)

```bash
docker exec -it broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
docker exec -it broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic cli-demo --partitions 3 --replication-factor 2
docker exec -it broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic cli-demo --partitions 3 --replication-factor 1
docker exec -it broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic cli-demo
```

- [ ] `--list`: empty or only internal topics (auto-create is off).
- [ ] RF 2: **error**, expected wording close to `InvalidReplicationFactorException: Replication factor: 2 larger than available brokers: 1.`
  This failure is part of the lab, so it should happen.
- [ ] RF 1: `Created topic cli-demo.`
- [ ] `--describe`: `PartitionCount: 3  ReplicationFactor: 1`, three rows with `Leader: 1  Replicas: 1  Isr: 1`.
- **If RF 2 does *not* fail:** note it; then question 4 in `01_kafka_cli.md` and the model answer need a change.

### Step 5 – Keyed console producer and consumer (1–2 min)

T1 (producer). Type the lines, then press Ctrl+C:

```bash
docker exec -it broker /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic cli-demo --reader-property parse.key=true --reader-property key.separator=:
```

```
tirol:hello
wien:servus
tirol:again
```

T2 (consumer). Press Ctrl+C once you have checked the output:

```bash
docker exec -it broker /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic cli-demo --from-beginning --formatter-property print.key=true --formatter-property print.partition=true --formatter-property print.offset=true
```

- [ ] **Expected:** all three messages show up with key, partition and offset. Both `tirol` lines are in the **same**
  partition (the order across partitions may differ).
- **If it fails:** "No such property parse.key" / "formatter-property not recognized" means the image is older than the
  CLI syntax in the lab. Check `docker exec broker /opt/kafka/bin/kafka-topics.sh --version`; it should be 4.4.0.

### Step 6 – Consumer group and lag (1 min)

```bash
docker exec -it broker /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic cli-demo --group cli-group --from-beginning
# Ctrl+C after the 3 messages, then:
docker exec -it broker /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group cli-group
docker exec -it broker /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
```

- [ ] **Expected:** `--describe` lists the 3 partitions with `LAG 0` (it may also say the group has no active members, which is fine).
- [ ] `--list` contains `cli-group`.

### Step 7 – Log segments and hexdump (2 min) – the riskiest step

```bash
docker exec -it broker bash -lc "ls /tmp/kafka-logs"
docker exec -it broker bash -lc "ls -la /tmp/kafka-logs/cli-demo-0"
docker exec -it broker /opt/kafka/bin/kafka-dump-log.sh --files /tmp/kafka-logs/cli-demo-0/00000000000000000000.log --print-data-log
docker exec -it broker bash -lc "hexdump -C /tmp/kafka-logs/cli-demo-0/00000000000000000000.log | head -20"
docker cp broker:/tmp/kafka-logs/cli-demo-0/00000000000000000000.log ./segment.log
```

- [ ] `ls` shows folders `cli-demo-0`, `cli-demo-1`, `cli-demo-2` (and `__consumer_offsets-*`).
- [ ] The partition folder has `00000000000000000000.log`, `.index`, `.timeindex`.
- [ ] `dump-log` prints batches with `payload:` lines. A partition can be empty; if so, try `cli-demo-1` / `-2`.
- [ ] `hexdump` shows hex plus readable text such as `hello`.
- [ ] `docker cp` creates `./segment.log` on the Mac.
- **If `bash` is missing** ("executable file not found"): replace `bash -lc` with `sh -c` in all three lines, and change
  `01_kafka_cli.md` lines 188–191 the same way.
- **If `hexdump` is missing** inside the container: run `hexdump -C segment.log | head -20` **on the Mac** after `docker cp`
  (macOS has hexdump), or use the Python fallback in `01_kafka_cli.md` (section after line 204). Adjust the lab text so
  students run hexdump after `docker cp`.
- Afterwards, delete `segment.log` so it does not end up in a commit.

### Step 8 – Add a partition (20 s)

```bash
docker exec -it broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --alter --topic cli-demo --partitions 4
docker exec -it broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic cli-demo
```

- [ ] **Expected:** `PartitionCount: 4`. A later `--partitions 2` would fail, because partitions cannot be reduced.

### Step 9 – Python clients against the Docker broker (3 min)

From the repo root, once:

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements-local.txt
python -c "import kafka, faker; print(kafka.__version__)"
```

- [ ] **Expected:** a version starting with `3.` (a 2.0.x version fails on Python 3.12).

Then in `session-06/`. In T2 and T3, `source ../.venv/bin/activate` first:

```bash
# T2
python consumer.py --group group-a --name C1
# T3 (start about 5 s after C1)
python consumer.py --group group-a --name C2
# T4
python producer.py --count 20 --interval 0.5
```

- [ ] The producer creates `user-events` with 3 partitions and prints the sent messages.
- [ ] **Expected split (from the sandbox test):** C1 first owns `[0, 1, 2]`. After C2 joins, the group rebalances, e.g.
  C1 `[2]` and C2 `[0, 1]`. Each message appears in **one** consumer only.
- [ ] Keys map to fixed partitions. Sandbox mapping: p0 = Tirol, Kärnten, Burgenland, Steiermark, Niederösterreich;
  p1 = Wien, Salzburg, Oberösterreich; p2 = Vorarlberg.
- [ ] Stop C2 with Ctrl+C. C1 gets `[0, 1, 2]` back after a few seconds.
- [ ] Optional: `python consumer.py --group group-b --name B1 --from earliest` reads everything from the beginning.
- **If you see `NoBrokersAvailable`:** the broker is not up (Step 2) or something other than Docker answers on 9092.
  A hostname `broker` that cannot be resolved means the client got the internal listener; the clients must use
  `localhost:9092` (the default).

### Step 10 – Notebook quick run (1 min, optional)

Open `02_kafka_python.ipynb` with the `.venv` kernel and run it top to bottom.

- [ ] It runs without errors: keyed messages show 0 order breaks, unkeyed show some.
- [ ] **Clear all outputs and save** before closing.

### Step 11 – Kafka UI (1 min, optional)

```bash
docker compose --profile ui up -d
```

- [ ] Open <http://localhost:8080>. The cluster appears with topics `cli-demo` and `user-events` and groups `cli-group`, `group-a`.
- **If the UI shows no cluster:** check `docker compose logs kafka-ui | tail -20`. It must reach `broker:19092`, the
  internal listener, which only works inside the compose network.

### Step 12 – Clean up and check git (1 min)

```bash
docker compose --profile ui down      # deletes all topics and messages (fine for the test)
cd .. && git status
```

- [ ] `git status` shows only the intended Session 6 changes: `session-06/` student files, `requirements-local.txt`,
  `.gitignore`, `README.md`. `teacher/`, `kafka-data/`, `segment.log` and `__pycache__/` are **not** listed.
- [ ] `02_kafka_python.ipynb` has no outputs. Check with:
  `grep -c '"output_type"' session-06/02_kafka_python.ipynb` → should print `0`.
- [ ] Commit and push from the Mac, then on GitHub do a "Sync fork" test with a student fork if you have time.

---

### What to do if X fails

| Symptom | Likely cause | Quick fix for tomorrow |
|---|---|---|
| `Cannot connect to the Docker daemon` | Docker Desktop not running | Start it; tell students to start it before 09:00 |
| `port is already allocated` (9092) | Native broker or old container still running | `lsof -i :9092`; `docker ps -a`; stop / `docker rm -f broker` |
| Container exits after start | Config error in `docker-compose.yml` | First `ERROR` in `docker compose logs broker`; send it to me |
| RF 2 is accepted | Unexpected broker behaviour | Change question 4 and model answer; mention it in class |
| `bash` not found in container | Minimal image | Use `sh -c` in lines 188–191 of `01_kafka_cli.md` |
| `hexdump` not found in container | Minimal image | `docker cp` first, then hexdump on the Mac / Python fallback |
| `NoBrokersAvailable` in Python | Broker down or wrong bootstrap | Step 2 again; use `localhost:9092` |
| kafka-python import or API error | Old kafka-python 2.0.x | `pip install -U "kafka-python>=3.0,<4"` |
| No rebalance visible with C2 | C2 started in a different group, or too early | Same `--group group-a`; wait for C1's assignment first |
| UI empty | UI started without the broker | `docker compose --profile ui up -d` again after Step 2 |

If anything in the table happens, note the exact error text. Then the lab files can be fixed before Friday's part-time run.
