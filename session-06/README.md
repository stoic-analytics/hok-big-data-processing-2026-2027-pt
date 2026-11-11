# Session 6 – Kafka lab and exam preparation

Last session. In the first half you run a real **Kafka broker** on your laptop and work with it from the command line
and from Python. In the second half we prepare for the **written exam on 27.11.2026** (60 min, 60 points, no AI).

| Time | Block | What | File |
|---|---|---|---|
| 09:00–09:10 | – | arrival, **broker check** (does `localhost:9092` answer?) | this README |
| 09:10–10:10 | 1 | Broker & CLI: topics, partitions, keys, consumer groups, lag, log segments on disk | `01_kafka_cli.md` |
| 10:10–10:25 | | break | |
| 10:25–11:25 | 2 | Kafka from Python: producers, consumers, offsets, consumer-group experiments | `02_kafka_python.ipynb`, `producer.py`, `consumer.py` |
| 11:25–11:35 | | break | |
| 11:35–12:25 | 3 | Exam circuit: 6 stations covering Sessions 1–5, in pairs | `03_exam_circuit.md` (printed) |
| 12:25–12:55 | 4 | 15-minute mock exam slice, reviewed together · exam logistics | `04_mock_exam.md` |

**AI rule for the labs:** AI assistants are allowed, but you must be able to explain every line.
The exam is without AI.

---

## ✅ Before the session (at home, ~20 min)

Do this **before** we meet. If something fails, write down the error message and bring it – or plan to pair up (see below).

### 1. Get the newest material and update your environment
```bash
git pull                                           # after "Sync fork" on GitHub
```
Run the next commands in the **repo root** (where `requirements-local.txt` and `.venv/` are), **not** in `session-06/`.
From `session-06/` you get `Could not open requirements file` – then use `pip install -r ../requirements-local.txt`.

macOS / Linux:
```bash
source .venv/bin/activate
pip install -r requirements-local.txt              # new: kafka-python and faker
```
Windows (PowerShell):
```powershell
.venv\Scripts\Activate.ps1
pip install -r requirements-local.txt
```
Check: `python -c "import kafka, faker; print(kafka.__version__)"` → `3.x`.
(Old tutorials use `kafka-python==2.0.2` – it **does not work on Python 3.12**.)

### 2. Start a Kafka broker – Route A (Docker, recommended)
1. Install **Docker Desktop** (docker.com → Get Started; Windows needs WSL 2, the installer offers it) and start it.
2. Download the image once at home (~240 MB download, the campus Wi-Fi will thank you):
   ```bash
   docker pull apache/kafka:4.4.0
   ```
3. Go **into** the `session-06/` folder first – `docker-compose.yml` lives there, not in the repo root:
   ```bash
   cd session-06
   docker compose up -d
   docker compose ps                  # broker should be "running"
   ```
4. Optional web UI: `docker compose --profile ui up -d` → http://localhost:8080 (another ~220 MB download).

### 2'. … or Route B (native Kafka, no Docker)
For macOS, Linux, WSL or the Codespace (it has Java 17 but no Docker). Needs **Java 17 or newer**
(`java -version`; macOS: `brew install openjdk@17`, Ubuntu: `sudo apt install openjdk-17-jre-headless`).

```bash
cd session-06
python get_kafka.py --start           # downloads Kafka 4.4.0 (~130 MB), checks the SHA-512, formats storage, starts the broker
```
Leave that terminal open – **Ctrl+C stops the broker**. Next time just run `python get_kafka.py --start` again.

⚠️ **The folder path must not contain a space** (e.g. `…/My Projects/…` or `… Kopie`). Kafka's scripts then fail with
`Classpath is empty`. `get_kafka.py` checks this and stops; install somewhere else instead:
```bash
python get_kafka.py --dest ~/kafka-course --start
```
Windows without WSL: use Route A.

### 3. Check the broker
With the broker running, in the activated `.venv`:
```bash
python -c "from kafka import KafkaAdminClient; print(KafkaAdminClient(bootstrap_servers='localhost:9092').list_topics())"
```
A list (maybe empty `[]`) = ✓. `KafkaTimeoutError: Unable to bootstrap` = the broker is not running or not on port 9092.

### Your laptop cannot run either route?
No problem: **pair up** with a neighbour whose broker runs. You type on their laptop in Block 1 and run `consumer.py` on
theirs in Block 2. Everything in Blocks 3 and 4 needs only paper.
(Running the scripts on your laptop against *their* broker does not work with our setup – the broker tells every client to connect to `localhost`, i.e. to the client's own laptop.)

---

## 🧹 Stop and clean up

| | Stop | Delete all topics and messages |
|---|---|---|
| Route A | `docker compose stop` (start again: `docker compose start`) | `docker compose down` – the next `up -d` starts empty |
| Route B | Ctrl+C in the broker terminal | `python get_kafka.py --reformat` (plus `--dest …` if you used it) |

Disk space: the image download is ~240 MB (`docker image rm apache/kafka:4.4.0` removes it);
Route B uses ~140 MB in `kafka_2.13-4.4.0/` + `kafka-data/` – both are ignored by git.

---

## 🛠️ Troubleshooting

| Symptom | Fix |
|---|---|
| `Cannot connect to the Docker daemon` / `docker: command not found` | Start Docker Desktop and wait until it says *running*; open a new terminal |
| `no configuration file provided: not found` | You are in the repo root → `cd session-06`, then `docker compose up -d` again. All `docker compose` commands (also `stop`/`down`) must run in `session-06/` |
| `Could not open requirements file: requirements-local.txt` | It lives in the repo root → `cd ..` first, or `pip install -r ../requirements-local.txt` |
| `consumer.py` prints `ASSIGNED partitions []` (several times) | Topic `user-events` does not exist yet – normal if you start the consumer first. Start `producer.py` (it creates the topic); the consumer gets its partitions within a few seconds |
| `can't open file '…/producer.py'` | You are in the repo root → `cd session-06` |
| `port is already allocated` (9092 or 8080) | Another Kafka is running: `docker ps`, stop it – or stop your Route B broker |
| `Classpath is empty` | The Kafka folder path contains a space → `python get_kafka.py --dest ~/kafka-course` |
| `No working Java found` / `UnsupportedClassVersionError` | Install Java 17+ (Route B), or use Docker |
| Broker refuses to start after changing the Kafka version | `python get_kafka.py --reformat` |
| `ModuleNotFoundError: kafka.vendor.six.moves` | You have the old `kafka-python 2.0.2` → `pip install -r requirements-local.txt` |
| `UnknownTopicOrPartitionError` / topic does not exist | Our broker does not auto-create topics – create it first (Block 1, part 2). `producer.py` creates `user-events` itself |
| Notebook: `ModuleNotFoundError: kafka_data` | Open the notebook from the `session-06/` folder |
| Apple Silicon (M1–M4) | Nothing special – the image and Kafka run natively |

---

## Files in this folder

| File | What |
|---|---|
| `README.md` | this page |
| `docker-compose.yml` | Route A: one Kafka 4.4.0 broker (KRaft) + optional web UI |
| `get_kafka.py` | Route B: downloads and prepares native Kafka 4.4.0 (standard library only) |
| `kafka_data.py` | fake data: `get_registered_user()` and keyed events with a per-key sequence number |
| `producer.py`, `consumer.py` | terminal producer / consumer for the consumer-group experiments |
| `01_kafka_cli.md` | Block 1 – step by step, both routes |
| `02_kafka_python.ipynb` | Block 2 – notebook |
| `03_exam_circuit.md` | Block 3 – station sheet |
| `04_mock_exam.md` | Block 4 – 60-minute mock exam (we do a 15-minute slice in class, the rest is for home) |
