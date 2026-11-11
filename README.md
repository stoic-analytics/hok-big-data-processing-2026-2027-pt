# Big Data Processing – FH Kufstein Tirol (DSIA, 3rd semester) · Winter Semester 2026/27

Hands-on material for the course. You work in **two places**:

| Where | What runs there |
|---|---|
| **Your laptop** – VS Code + Python 3.12 in a virtual environment | pandas, Polars, DuckDB, file formats, scikit-learn, Kafka (Session 6) |
| **Kaggle** – free notebooks in the browser | Apache Spark and Spark ML (no Java setup on your laptop) |

**AI rule for the labs:** AI assistants are allowed, but you must be able to explain every line.
The exam is without AI.

**Written exam: 27.11.2026** – 60 min, 60 points, no AI. Preparation material: [`session-06/03_exam_circuit.md`](session-06/03_exam_circuit.md)
and [`session-06/04_mock_exam.md`](session-06/04_mock_exam.md).

---

## Before every session

1. On GitHub: open **your fork** → **Sync fork**.
2. In the repository folder on your laptop:
   ```bash
   git pull
   python get_data.py --months 2      # Windows: py -3.12 get_data.py --months 2
   ```
   `get_data.py` uses only the Python standard library, so it works even before you set up an environment.
   It downloads NYC Yellow Taxi trips 2024 (Parquet, ~50 MB per month) and the taxi zone lookup into `data/`.

---

## One-time setup on your laptop (Windows, macOS, Linux)

### 1. Install the tools
| | Windows | macOS | Linux (Ubuntu/Debian) |
|---|---|---|---|
| **VS Code** + extensions **Python** and **Jupyter** | code.visualstudio.com | code.visualstudio.com | code.visualstudio.com |
| **Git** | `winget install Git.Git` | `xcode-select --install` | `sudo apt install git` |
| **Python 3.12** | `winget install Python.Python.3.12` (tick “Add to PATH” in the python.org installer) | `brew install python@3.12` | `sudo apt install python3.12 python3.12-venv` |

### 2. Fork and clone
On this repository: **Fork → Create fork**. Then:
```bash
git clone https://github.com/<your-username>/<this-repository>.git
cd <this-repository>
```

### 3. Create the virtual environment
macOS / Linux:
```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements-local.txt
```
Windows (PowerShell):
```powershell
py -3.12 -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements-local.txt
```
Then in VS Code: **Ctrl/Cmd + Shift + P → “Python: Select Interpreter” → `.venv`**, and in every notebook
choose the kernel **`.venv`** (top right).

Check everything with `python session-04/check_env.py`.
Step-by-step explanations and troubleshooting: [`session-04/01_python_environments.md`](session-04/01_python_environments.md)
and [`session-04/README.md`](session-04/README.md).

### Which requirements file?
| File | Contains | Use it for |
|---|---|---|
| `requirements-local.txt` | pandas, Polars, DuckDB, PyArrow, scikit-learn, plots, kafka-python 3.x + Faker (Session 6) | **your laptop** (default) |
| `requirements.txt` | the above **+ PySpark 4.0 and Delta Lake** (needs Java 17+) | the dev container, or a laptop with Java 17 installed |

---

## Spark labs on Kaggle

1. Create a free account on kaggle.com and **verify your phone number** (Settings) – otherwise notebooks have no internet.
2. **Create → New Notebook → File → Import Notebook** → upload the notebook from your local repository.
3. Right panel: **Input → + Add Input** → the dataset named at the top of the notebook.
4. **Settings → Internet: On · Accelerator: None**.
5. Run the cells from the top. The first cell installs Java 17 and PySpark if Kaggle does not have them.

---

## Alternative: dev container (GitHub Codespaces or Docker)

The repository also contains a dev container with Python 3.12, **Java 17** and everything from `requirements.txt` –
besides Kaggle, the way to run Spark without installing Java yourself.

- **Codespaces:** on your fork **Code → Codespaces → Create codespace on main**; ~3 min for the first build.
  Stop it when you are done (github.com/codespaces → ⋯ → Stop) – otherwise it uses up your free hours.
  More hours: GitHub Student Developer Pack (education.github.com).
- **Local Docker:** Docker Desktop + VS Code extension **Dev Containers** → “Reopen in Container”.

A Codespace has 8 GB RAM and effectively one physical CPU core: fine for learning Spark, not for speed comparisons.

---

## Kafka lab (Session 6)

Session 6 runs a real Kafka broker on your laptop. Set it up **at home before the session** – full instructions,
broker check and troubleshooting: [`session-06/README.md`](session-06/README.md).

1. Update your environment: activate `.venv`, then `pip install -r requirements-local.txt` (adds **kafka-python 3.x** and **Faker**;
   the old `kafka-python 2.0.2` does not work on Python 3.12).
2. Start a broker – choose **one** route:

| Route | Needs | Start (in `session-06/`) |
|---|---|---|
| **A – Docker** | Docker Desktop | `docker compose up -d` (broker image `apache/kafka:4.4.0`, ~240 MB; optional web UI: `docker compose --profile ui up -d`) |
| **B – native Kafka** | Java 17+ (macOS, Linux, WSL or the Codespace) | `python get_kafka.py --start` (downloads Kafka 4.4.0, ~130 MB) |

⚠️ **Route B fails if the folder path contains a space** (e.g. `… Kopie` or `My Projects`). `get_kafka.py` checks this and stops –
then install Kafka somewhere else: `python get_kafka.py --dest ~/kafka-course --start`.

Your Python code connects to `localhost:9092`. If neither route works on your laptop, pair up with a neighbour.

---

## Sessions and folders

| Folder | Topic | Runs on |
|---|---|---|
| `session-01/` | Do you really need a cluster? Threads vs. processes, pandas vs. Polars vs. DuckDB, Parquet vs. CSV, bonus: Python without the GIL | laptop |
| `session-02/` | Apache Spark fundamentals: SparkSession, transformations vs. actions, plans, shuffles, joins | dev container or a laptop with Java 17 |
| *(Session 3 – no folder)* | Groups Work | Google Drive |
| `session-04/` | Reproducible Python (venv, pyenv, uv), CSV vs. pickle vs. Parquet, DuckDB as a database file, Spark fundamentals + Spark ML | laptop (Labs A–C) · Kaggle (Lab D) |
| *(Session 5 – no folder)* | Stream processing with Kafka – theory: events, append-only logs, topics and partitions, replication, KRaft | Kaggle |
| `session-06/` | Kafka lab: broker and CLI (`01_kafka_cli.md`), Python producers and consumers (`02_kafka_python.ipynb`, `producer.py`, `consumer.py`), consumer groups · exam preparation (`03_exam_circuit.md`, `04_mock_exam.md`) | laptop with Docker or native Kafka (Java 17+) |

Every lab ends with **exam-style questions** – they are the best preparation for the written exam.

---

## Never commit
`data/`, `.venv/`, executed notebooks with large outputs, or files with personal information.
The `.gitignore` already excludes data files, virtual environments and caches, as well as the Kafka download and broker data
from Session 6 (`session-06/kafka_2.13-*/`, `session-06/kafka-data/`).
