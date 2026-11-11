# Mock exam · model answers (NOT for the student repo)

60 P in 60 min, point split as agreed: Foundations 12 · Spark architecture/HDFS/MapReduce 16 ·
Environments/Formats/DuckDB 12 · Spark ML/Joins 10 · Kafka 10.

## ⏱ 15-minute in-class slice (Block 4) – 12 questions, 17 P

Hand out the full exam, students answer **only the ★ questions** in 15 min, then review live (~10 min).
One question per area at least; all short so 12 fit in 15 minutes.

| ★ | Question | Area | P |
|---|---|---|---|
| 1 | A1 – three Vs | Foundations | 1 |
| 2 | A5 – threads or processes | Foundations | 2 |
| 3 | B2 – blocks and replicas | HDFS | 2 |
| 4 | B7 – `count()` | Spark | 1 |
| 5 | B8 – task / stage | Spark | 2 |
| 6 | C1 – columnar formats | Formats | 1 |
| 7 | C5 – pickle | Formats | 2 |
| 8 | D1 – narrow or wide | Spark/Joins | 1 |
| 9 | D4 – CrossValidator fits | Spark ML | 1 |
| 10 | E1 – KRaft | Kafka | 1 |
| 11 | E2 – offset scope | Kafka | 1 |
| 12 | E3 – 3 partitions, 5 consumers | Kafka | 2 |

---

## Part A – Foundations (12 P)

**A1 ★ (1 P)** Volume, Velocity, Variety (all three for the point).

**A2 (1 P)** Veracity.

**A3 (2 P)** Scale up: physical hardware limit / disproportionate cost of high-end servers / single point of failure (1 P).
Scale out: network costs, partial failures, coordination, stragglers, more complex code (1 P).

**A4 (2 P)** A mutex in CPython that lets only **one thread execute Python bytecode at a time** (1 P). It prevents
CPU-bound Python code from running in parallel on several cores with threads (it protects reference counting /
memory management) (1 P).

**A5 ★ (2 P)** (a) Threads – I/O-bound, the GIL is released while waiting (1 P). (b) Processes – CPU-bound, each
process has its own GIL, n_processes = n_cores (1 P).

**A6 (4 P)** Against the cluster (or "not yet"): 8 GB Parquet fits on one machine – DuckDB or Polars (lazy, all cores,
DuckDB spills to disk) can aggregate it, only needed columns are read (1 P tool + 1 P reason). Distributed systems are
harder: machines fail, network is slow / moving data costs more than computing, coordination, stragglers (1 P for two).
Cluster justified when data or compute exceeds one machine (e.g. many TB, or the data already lives in a distributed
store / data lake, or strict time limits that one machine cannot meet) (1 P). Rule: scale up first.

---

## Part B – Spark architecture, HDFS, MapReduce (16 P)

**B1 (1 P)** NameNode.

**B2 ★ (2 P)** 3 blocks (64 + 64 + 22 MB) (1 P), 9 block copies (1 P).

**B3 (2 P)** DataNodes send regular heartbeats; missing heartbeats → NameNode marks the node dead (1 P). Each block has
replicas on other DataNodes; the NameNode re-replicates the missing copies to restore the factor 3 (1 P).

**B4 (3 P)** Map: (big,1)(data,1)(is,1)(big,1) / (data,1)(is,1)(fast,1) (1 P). Shuffle & sort: big [1,1], data [1,1],
fast [1], is [1,1] (1 P). Reduce: big 2, data 2, fast 1, is 2 (1 P).

**B5 (2 P)** Any two: intermediate results are written to disk between steps (no in-memory caching) → slow for iterative
algorithms like ML; batch only, not real-time; complex low-level programming model (Java map/reduce classes) vs.
Spark's DataFrame/SQL API (1 P each).

**B6 (3 P)** Driver: SparkSession, builds and optimises the plan, splits into stages/tasks, schedules, collects results.
Executors: run the tasks on the workers, keep cached partitions. Cluster manager: allocates the machines/containers for
executors (YARN, Kubernetes, standalone; `local[*]` = all in one JVM). (1 P each.)

**B7 ★ (1 P)** Action (it triggers a job).

**B8 ★ (2 P)** Task = the unit of work on **one partition**, run by one core of an executor (1 P). A new stage starts at
a **shuffle** = wide transformation (groupBy, join, orderBy; `Exchange` in the plan) (1 P).

---

## Part C – Python environments, file formats, DuckDB (12 P)

**C1 ★ (1 P)** Parquet and ORC.

**C2 (1 P)** Avro.

**C3 (2 P)** pyenv only selects the **Python interpreter version** (1 P); all projects on that version still share one
`site-packages`, so package versions collide – venv isolates packages per project; they complement each other (1 P).

**C4 (2 P)** `requirements.txt`: direct dependencies with ranges (e.g. `pandas>=2.2,<3`) – a new install can pick newer
versions (1 P). Lock file: exact versions of **all** packages incl. transitive ones → identical environment (1 P).

**C5 ★ (2 P)** Unpickling can run arbitrary code (`__reduce__`), e.g. `os.system(...)` – loading = executing (1 P).
Only load pickles you created or fully trust; share data as Parquet (1 P). (Also acceptable: needs identical library versions.)

**C6 (2 P)** It runs as a library **inside your Python process**, no server; the database is one file (1 P). Only **one
process can write** to the file at a time (others get a lock error) – not for many concurrent writers (1 P).

**C7 (2 P)** Projection pushdown: read only the needed **columns** (1 P). Predicate pushdown: apply filters while
reading, skip **rows/row groups** (using min/max statistics) (1 P).

---

## Part D – Spark ML and joins (10 P)

**D1 ★ (1 P)** Wide – needs a shuffle (`Exchange`).

**D2 (2 P)** The zone table is tiny (below `spark.sql.autoBroadcastJoinThreshold`, 10 MB) → copied to every executor,
the 3 million trips are joined locally without a shuffle (1 P). Broadcasting a huge table: it must be collected on the
driver and copied to every executor → out-of-memory errors / very slow (1 P).

**D3 (2 P)** StringIndexer = Estimator, VectorAssembler = Transformer, LinearRegression = Estimator (1 P for all three);
`Pipeline.fit()` returns a **PipelineModel** (a Transformer) (1 P).

**D4 ★ (1 P)** 3 × 2 × 5 = 30 + 1 refit = **31**.

**D5 (2 P)** Using information as a feature that is not available at prediction time / is derived from the target (1 P).
Example: `fare_amount`, `total_amount`, `tip_amount` or the dropoff time when predicting duration at pickup (1 P).

**D6 (2 P)** Two shuffles (two `Exchange hashpartitioning`) (1 P). Spark chose SortMergeJoin, so **neither table** was
small enough to broadcast (both above the threshold) – both sides are shuffled by `customer_id` and sorted (1 P).

---

## Part E – Kafka and streaming (10 P)

**E1 ★ (1 P)** KRaft (Kafka Raft metadata mode). ZooKeeper is removed in Kafka 4.

**E2 ★ (1 P)** Within a partition.

**E3 ★ (2 P)** Each partition goes to exactly one consumer of the group → 3 consumers get one partition each (1 P);
2 consumers are **idle** and take over after a rebalance if one fails (1 P).

**E4 (2 P)** Order is guaranteed **only within a partition** (1 P). Use the customer id as the **message key** → all events
of that customer go to the same partition (1 P).

**E5 (2 P)** ISR = in-sync replicas, the copies fully caught up with the leader (1 P). On leader failure a new leader is
elected from the ISR (by the KRaft controller); with `acks=all` acknowledged data is not lost (1 P).

**E6 (2 P)** Lambda: batch layer + speed layer in parallel, merged in a serving layer (½ P). Kappa: one stream-processing
layer for everything (½ P). Kafka in Kappa: the durable, replayable append-only log – reprocessing = re-reading the
topic from the beginning (1 P).
