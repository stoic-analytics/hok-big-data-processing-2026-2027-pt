# Session 6 – Teacher guide (NOT for the student repo)

**Groups:** vz Wed 11.11.2026 · bb Fri 13.11.2026 · 09:00–13:00 · identical content
**Title:** Kafka lab + exam preparation
**Exam:** Fri 27.11.2026, 60 min, 60 P, no AI. Split: Foundations 12 · Spark architecture/HDFS/MapReduce 16 ·
Environments/Formats/DuckDB 12 · Spark ML/Joins 10 · Kafka 10.

---

## 1. Before Wednesday – checklist for Mario

| # | To do | Why |
|---|---|---|
| 1 | Copy the student zip into your repo, check `git status`: `session-06/teacher/`, `kafka_2.13-*`, `kafka-data/` must **not** appear (new `.gitignore` lines) | solutions and binaries stay out |
| 2 | Push from your Mac **today** and send students the "Before the session" part of `session-06/README.md` | `docker pull` (~240 MB) and `pip install` at home, not on campus Wi-Fi |
| 3 | **Run Route A once on your Mac**: `cd session-06 && docker compose up -d`, then Block 1 parts 1–6 with the `docker exec` commands | ⚠ I could not run Docker in my sandbox – the compose file and all `docker exec` commands are **untested** (see section 8) |
| 4 | Optional: `docker compose --profile ui up -d` → http://localhost:8080 | kafbat/kafka-ui v1.5.0 replaces the unmaintained provectuslabs image – untested |
| 5 | Route B on your Mac: your copy of the repo is in a folder with a space (`… Kopie`) → `python get_kafka.py --dest ~/kafka-course --start` | Kafka scripts fail with "Classpath is empty" in paths with spaces; the script now stops with a clear message |
| 6 | Print `03_exam_circuit.md`: one sheet per pair (≈ 15 per group) + 1 copy of each station on the station tables | Block 3 |
| 7 | Print `04_mock_exam.md` (one per student) or show it on screen | Block 4 slice |
| 8 | Have the `teacher/*_MODEL_ANSWERS.md` open on your tablet | |

**Repo changes in this delivery:**
- New `session-06/`: README, `docker-compose.yml`, `get_kafka.py`, `kafka_data.py`, `producer.py`, `consumer.py`,
  `01_kafka_cli.md`, `02_kafka_python.ipynb` (no outputs), `03_exam_circuit.md`, `04_mock_exam.md`.
- `requirements-local.txt`: `kafka-python>=3.0,<4` and `faker` (Session 6 comment). `kafka-python 2.0.2` from your old lab
  **fails on Python 3.12** (`kafka.vendor.six.moves`).
- `.gitignore`: `session-06/kafka_2.13-*/`, `session-06/kafka-data/`, `session-*/teacher/`.
- Root `README.md`: session table now lists session-05 (Kafka theory) and session-06; the "Coming: Kafka" line is removed.

**What changed vs. your old lab (`kafka.zip`):** the old notebooks are the base. `producer.py`/`consumer.py` are the
same KafkaProducer/KafkaConsumer settings (acks=all, retries=5, linger_ms=10 · earliest, auto-commit) as terminal scripts
with argparse, keys, partition/offset output and a rebalance listener. `get_registered_user()` is unchanged in
`kafka_data.py`; `KeyedEvents` adds keys (9 Austrian states) and a per-key `seq`. The compose file is your file with:
`apache/kafka:4.4.0` (instead of `4.2.0-rc3`), explicit `KAFKA_LOG_DIRS=/tmp/kafka-logs` (matches your slides),
auto-topic-creation off, 1 MB segments, share-coordinator RF 1, kafbat UI behind the profile `ui`.

---

## 2. Run sheet (09:00–13:00)

| Time | Min | Block | Mode | Notes |
|---|---|---|---|---|
| 09:00 | 10 | Arrival, **broker check** (README step 3) | check | hands up: Route A / Route B / nothing → form pairs **now** |
| 09:10 | 5 | Intro Block 1: partition, key, offset, group, lag (1 slide from Session 5: "Producers evenly hash data to partitions") | input | |
| 09:15 | 50 | **Block 1** `01_kafka_cli.md` parts 0–7 | lab, pairs | escape hatch: show part 6 (disk) on the projector at 09:55 if the room is behind |
| 10:05 | 5 | Block 1 exam questions 2 and 5 in plenary | plenary | |
| 10:10 | 15 | Break | | Route A students: `docker compose ps` still running? |
| 10:25 | 10 | Intro Block 2: serializer, acks, auto-commit, at-least-once | input | |
| 10:35 | 25 | **Block 2** notebook sections 0–6 | lab | slow? skip section 4 (unkeyed) and show it on the Mac |
| 11:00 | 20 | **Block 2 part 7** terminal experiments 1–3 | lab + projector | do experiment 2 **live** on the Mac with 2 terminals side by side, students repeat |
| 11:20 | 5 | Block 2 exam questions 2 and 3 | plenary | |
| 11:25 | 10 | Break | | lay out station sheets |
| 11:35 | 6×6 | **Block 3** exam circuit, rotate every 6 min | pairs | debrief after stations 2, 4, 6 (≈ 4 min each, only the questions most pairs got wrong) |
| 12:25 | 15 | **Block 4** mock slice: 12 ★ questions | individual, exam conditions | no laptops |
| 12:40 | 10 | Review the slice live | plenary | let students mark their own sheet |
| 12:50 | 5–10 | Exam logistics, questions, feedback | | see section 6 |

---

## 3. Setup risks and fallbacks

| Risk | Sign | Fallback |
|---|---|---|
| Docker Desktop not installed / Windows without WSL 2 / old Intel Mac | `docker: command not found`, WSL error | pair up (README "Your laptop cannot run either route?"); Route B in WSL if it exists |
| Image not pulled at home, Wi-Fi slow | `docker compose up` hangs on pulling | pair up; or hotspot; image ~240 MB |
| Port 9092 taken (second Kafka, old container) | `port is already allocated` | `docker ps` → `docker stop <id>`; or stop the Route B broker |
| Route B without Java 17 | script stops: "No working Java found" | Docker, or pair |
| Route B path with a space | script stops with the space message | `--dest ~/kafka-course` |
| Old `kafka-python 2.0.2` in `.venv` | `ModuleNotFoundError: kafka.vendor.six.moves` | `pip install -r requirements-local.txt` |
| Codespace students | no Docker-in-Docker | Route B works there (Java 17 present) – producer/consumer in the Codespace terminal |
| Students try to connect to a neighbour's broker | timeouts after first connect | does not work by design (advertised listener is `localhost`) – they must type on the neighbour's laptop |
| Topic `user-events` created earlier with 1 partition | notebook prints the ⚠ line, only one consumer gets messages | delete the topic (Block 1 part 7 command with `user-events`) and rerun the notebook cell |

---

## 4. Block 1 – expected outputs (observed on native Kafka 4.4.0)

See `01_kafka_cli_MODEL_ANSWERS.md` for every prediction and the outputs. Key facts to check on students' screens:
- Observed key → partition mapping with 3 partitions: Tirol/Kaernten/Burgenland/Steiermark/Niederoesterreich → 0,
  Wien/Salzburg/Oberoesterreich → 1, Vorarlberg → 2 (same for CLI and Python).
- Kafka 4.4 marks `--property` as deprecated → the lab uses `--reader-property` (producer) and `--formatter-property`
  (consumer). Your Session 5 slides do not use either, so no conflict.
- `kafka-console-consumer` prints a harmless hint about KIP-848 (new consumer rebalance protocol).
- The `.index`/`.timeindex` files show 10 MB (pre-allocated) – students will ask.

---

## 5. Block 2 – expected outputs of the experiments (observed)

**Notebook** (executed top to bottom twice against the native broker, outputs stripped again):
section 3 every key in exactly one partition; section 4 unkeyed spread over all partitions (kafka-python picks a
partition per message); section 5 order violations: **keyed 0**, **unkeyed 8–10** in my runs; section 6 same group
second read **0 records**, lag after 5 new messages **total 5**.

**Experiment 2 – second consumer, same group** (observed console output):
```
[C1] ASSIGNED partitions [0, 1, 2]
[C1] REVOKED  [0, 1, 2]
[C1] ASSIGNED partitions [2]
[C2] ASSIGNED partitions [0, 1]
```
`kafka-consumer-groups.sh --describe --group group-a` then showed two different CONSUMER-IDs: one owning partitions 0 and 1,
the other partition 2. Directly after the burst the LAG column showed 5/5/3 although both consumers had printed all
messages – **auto-commit runs every 5 s**, a nice moment to explain at-least-once: describe again after a few seconds → 0.

**Stopping C2 with Ctrl+C** (observed): C2 prints `REVOKED [2]`, C1 prints `REVOKED [0, 1]` then `ASSIGNED partitions [0, 1, 2]`
and continues with partition 2 at the committed offset. 4 consumers on 3 partitions → one prints `ASSIGNED partitions []`
(idle) – expected, but I only tested 2 consumers.

**Experiment 3 – different group** (observed): `group-b` got `ASSIGNED [0, 1, 2]` and read all 33 existing messages from
offset 0 (14/15/4 per partition), independent of group-a.

**Bonus `--no-key`:** consumers may print `⚠ expected seq …` for a state whose events now sit in different partitions.

---

## 6. Block 4 – exam logistics (fill in what is still open)

- Date: **Fri 27.11.2026**, 60 minutes, 60 points, no AI, no notes. Room / start time: _____ (t.b.d. by you).
- Question types as in the mock: short definitions (1–2 P), explain-why (3–4 P), scenario + justification (4+ P),
  one "read the artefact" task (plan / command output).
- Point split per part (table above). Best preparation: all "Exam-style questions" at the end of every lab +
  the full mock exam at home under exam conditions.
- Mock exam model answers: decide whether you publish them after 13.11 (they are not in the student zip).

---

## 7. Between 11.11 (vz) and 13.11 (bb) – what to check

| Check | Action for Friday |
|---|---|
| How many students could run Route A / Route B / neither? | if many failed: send the bb group a reminder Thursday to `docker pull` + test at home |
| Did the compose file and all `docker exec` commands work on Wednesday? Note any error message | send me the error → fix before Friday |
| Did kafbat UI start? | if not: remove it from the pre-session note |
| Timing of Block 1 (50 min) and the circuit (6 min per station) | cut Block 1 part 7 or the circuit debriefs if over |
| Which circuit questions did most pairs get wrong? | add those to the Block 4 review on Friday |
| `hexdump` inside the container (Alpine/busybox) available? | if not: use the Python snippet after `docker cp` for everyone |

---

## 8. What was tested – and what not

| Item | Status |
|---|---|
| `get_kafka.py`: space guard (stops on the repo path with a space), Java check, config writing, `kafka-storage format --standalone`, `--start` from a space-free `--dest` | **tested** (Kafka 4.4.0, Java 17, Linux) |
| `get_kafka.py` download + SHA-512 | checksum lookup tested (official .sha512 parsed); the 130 MB download itself **not** re-run (distribution already present) |
| All native commands in `01_kafka_cli.md` (parts 1–7, incl. RF-2 error, lag, dump-log, hexdump, Python hex snippet, alter 3→4, decrease error, delete) | **tested** |
| `producer.py` / `consumer.py`: keyed, `--no-key`, same group split + rebalance, consumer leaving, different group, Ctrl+C (SIGINT) clean shutdown | **tested** |
| `02_kafka_python.ipynb` and the SOLUTION notebook executed top to bottom (kafka-python 3.0.13, Python 3.12) | **tested**, student file saved without outputs |
| `docker-compose.yml`, all `docker exec …` / `docker cp` commands, kafbat UI, Windows/PowerShell | **untested** – no Docker in my sandbox; image tags `apache/kafka:4.4.0` and `kafbat/kafka-ui:v1.5.0` verified to exist on Docker Hub |
| `--data users` mode of `producer.py` (old lab data) | **not run** separately (`get_registered_user()` itself tested) |
