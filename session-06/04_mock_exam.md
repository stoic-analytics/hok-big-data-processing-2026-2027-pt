# Big Data Processing – Mock exam  (60 minutes · 60 points)

**Rules – same as on 27.11.2026:** no laptop, no phone, no AI, no notes. Answer in English, short and precise.
Points show the expected depth: **1 P** = one word or one fact, **2 P** = name/define or two facts,
**3–4 P** = explain why, **4+ P** = choose a solution and justify it.

In class we do a **15-minute slice** of this exam together. Do the rest at home under exam conditions: set a 60-minute timer.

| Part | Topic | Points |
|---|---|---|
| A | Foundations: Big Data, scaling, GIL, threads vs. processes | 12 |
| B | Spark architecture, HDFS, MapReduce | 16 |
| C | Python environments, file formats, DuckDB | 12 |
| D | Spark ML and joins | 10 |
| E | Kafka and streaming | 10 |
| | **Total** | **60** |

---

## Part A – Foundations (12 P)

**A1 (1 P)** Name the three original Vs of Big Data.

**A2 (1 P)** Which V describes how trustworthy and accurate the data is?

**A3 (2 P)** Name one disadvantage of *scaling up* and one disadvantage of *scaling out*.

**A4 (2 P)** What is the Global Interpreter Lock (GIL) in CPython, and what does it prevent?

**A5 (2 P)** Threads or processes? Give one word and a half-sentence reason for each:
(a) downloading 200 files from an API, (b) a pure-Python numerical loop that should use all 8 cores.

**A6 (4 P)** Your team has an 8 GB Parquet dataset (one year of trips) and laptops with 16 GB RAM and 8 cores.
A colleague wants to rent a Spark cluster for the weekly report. Argue for or against it: name a single-machine tool you would
try first and why it can work, two reasons why distributed systems are harder, and one situation in which the cluster
would be justified.

---

## Part B – Spark architecture, HDFS, MapReduce (16 P)

**B1 (1 P)** Which HDFS component stores the metadata (which blocks belong to which file, and where they are)?

**B2 (2 P)** A 150 MB file, block size 64 MB, replication factor 3. How many blocks does the file have, and how many
block copies are stored in the cluster?

**B3 (2 P)** A DataNode fails. How does HDFS notice it, and why is no data lost?

**B4 (3 P)** MapReduce word count on the input lines `big data is big` and `data is fast`.
Write down the map output, the result of shuffle & sort, and the reduce output.

**B5 (2 P)** Name two disadvantages of Hadoop MapReduce that Spark addresses.

**B6 (3 P)** Explain the roles of the driver, the executors and the cluster manager in a Spark application.

**B7 (1 P)** Is `df.count()` a transformation or an action?

**B8 (2 P)** In Spark, what is a *task*, and at which kind of operation does a new *stage* begin?

---

## Part C – Python environments, file formats, DuckDB (12 P)

**C1 (1 P)** Which of these formats are columnar: CSV, JSON, Avro, Parquet, ORC?

**C2 (1 P)** Which row-oriented binary format with an embedded schema is commonly used for Kafka events?

**C3 (2 P)** "I use pyenv, so I don't need venv." Explain why this is only half true.

**C4 (2 P)** What is the difference between a `requirements.txt` with version ranges and a lock file?

**C5 (2 P)** Why is it dangerous to load a pickle file from an unknown source?

**C6 (2 P)** Why is DuckDB called an *embedded* database? Name one consequence for concurrent writing.

**C7 (2 P)** Polars `scan_parquet` is lazy. Name the two pushdown optimisations and what each one saves.

---

## Part D – Spark ML and joins (10 P)

**D1 (1 P)** Narrow or wide transformation: `groupBy("PULocationID").agg(avg("fare_amount"))`?

**D2 (2 P)** Trips (3 million rows) are joined with the zone table (265 rows). Why does Spark choose a
BroadcastHashJoin, and what goes wrong if you force a broadcast of a very large table?

**D3 (2 P)** Transformer or Estimator: `StringIndexer`, `VectorAssembler`, `LinearRegression`.
What type of object does `Pipeline.fit()` return?

**D4 (1 P)** `CrossValidator` with a parameter grid of 3 × 2 values and `numFolds=5`. How many model fits in total,
including the final refit?

**D5 (2 P)** Define *target leakage* and give an example from the taxi trip-duration model.

**D6 (2 P)** Read this plan fragment:
```
SortMergeJoin [customer_id#3], [customer_id#21], Inner
:- Sort [customer_id#3 ASC NULLS FIRST], false, 0
:  +- Exchange hashpartitioning(customer_id#3, 200)
:     +- FileScan parquet orders[...]
+- Sort [customer_id#21 ASC NULLS FIRST], false, 0
   +- Exchange hashpartitioning(customer_id#21, 200)
      +- FileScan parquet customers[...]
```
How many shuffles are in this fragment? What does that tell you about the size of the two tables?

---

## Part E – Kafka and streaming (10 P)

**E1 (1 P)** What does Kafka 4 use instead of ZooKeeper to manage cluster metadata?

**E2 (1 P)** Is a Kafka offset unique within a topic or within a partition?

**E3 (2 P)** A topic has 3 partitions. One consumer group runs 5 consumers. How are the partitions assigned,
and what do the remaining consumers do?

**E4 (2 P)** Which ordering guarantee does Kafka give, and how do you make sure all events of one customer are processed in order?

**E5 (2 P)** Replication factor 3: what is the ISR, and what happens when the leader of a partition fails?

**E6 (2 P)** Describe the Lambda and the Kappa architecture in one sentence each. Which role does Kafka play in Kappa?
