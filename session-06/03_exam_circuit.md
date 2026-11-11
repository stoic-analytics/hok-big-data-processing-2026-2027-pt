# Block 3 – Exam circuit  (6 stations × ~6 min, in pairs)

**How it works:** every pair starts at a different station. Answer the three questions on paper, **without laptop and
without AI** – exactly like in the exam. When the signal comes, move to the next station (1 → 2 → … → 6 → 1).
We compare answers together at the end of each round of two stations.

Points show the expected depth: **2 P** = name/define, **3–4 P** = explain why, **4–6 P** = choose and justify.
Write in full sentences, but short. Total: 58 P.

Names: ______________________________  &  ______________________________

---

## Station 1 – Do you need a cluster?  (Session 2 · 9 P)

**1.1 (2 P)** Name the three original Vs of Big Data and explain **one** additional V (Variability, Veracity or Value)
with an example from the NYC taxi data.

**1.2 (3 P)** Compare *scale up* and *scale out*: one advantage and one disadvantage of each.
Which rule of thumb did we use in this course?

**1.3 (4 P)** Two jobs, one laptop with 8 cores:
(a) download 300 Parquet files from a web server, (b) compute a feature with a pure-Python loop over 50 million rows.
For each job: threads or processes? Justify with the GIL.

&nbsp;
&nbsp;

---

## Station 2 – File formats and engines  (Sessions 1, 2, 4 · 9 P)

**2.1 (2 P)** Sort into *row-oriented* and *column-oriented*: CSV, JSON, Avro, Parquet, ORC.
Which one is typical for writing events record by record to Kafka?

**2.2 (3 P)** A Parquet file and a CSV file contain the same 19 columns of 50 million trips.
Give three reasons why `avg(trip_distance)` is much faster on the Parquet file.

**2.3 (4 P)** Polars `scan_parquet(...).filter(...).select(...)` vs. pandas `read_parquet(...)` followed by the same
filter and selection. Explain *lazy evaluation* and the two optimisations *projection pushdown* and *predicate pushdown*.
Why can pandas not do them?

&nbsp;
&nbsp;

---

## Station 3 – HDFS, MapReduce, Spark architecture  (Session 3 · 10 P)

**3.1 (3 P)** A 150 MB file is stored in HDFS with 64 MB blocks and replication factor 3.
How many blocks, and how many block copies are stored in total? What does the NameNode store, what do the DataNodes store?

**3.2 (3 P)** Word count with MapReduce on the two lines `big data is big` and `data is fast`.
Write down (a) the output of the map phase, (b) what the shuffle produces, (c) the final reduce output.

**3.3 (4 P)** In a Spark application: what are the roles of the **driver**, the **executors** and the **cluster manager**?
One `count()` starts a *job* – how is it split into *stages* and *tasks*?

&nbsp;
&nbsp;

---

## Station 4 – Spark execution: lazy evaluation and shuffles  (Sessions 2, 4 · 10 P)

**4.1 (2 P)** Transformation or action? `filter`, `count`, `groupBy().agg()`, `show`, `withColumn`, `write.parquet`.

**4.2 (5 P)** Look at this PySpark code (the line numbers are not part of the code):
```python
1  df = spark.read.parquet("yellow_tripdata.parquet")
2  df = df.filter(col("trip_distance") > 0)
3  df = df.withColumn("distance_km", col("trip_distance") * 1.609)
4  result = df.groupBy("PULocationID").agg(avg("distance_km").alias("avg_km"))
5  result.show(5)
```
(a) After lines 1–4, Spark has not processed any trips yet. Why not? Which line starts the actual work? (2 P)
(b) Which of lines 2, 3 and 4 is a **wide** transformation? Explain why it needs a shuffle. (2 P)
(c) How many shuffles does this job have, and into how many **stages** does Spark split it? (1 P)

**4.3 (3 P)** Spark splits a 2 GB (= 2048 MB) file into partitions of 128 MB each. Each partition is processed by one
**task**. Your laptop has 4 cores, and each core works on one task at a time.
(a) How many partitions, and therefore tasks, does Spark create? (1 P)
(b) How many rounds ("waves") are needed until all tasks are done? (1 P)
(c) If the file had only 2 partitions, how many of the 4 cores would be busy? Name one function that fixes this. (1 P)

&nbsp;
&nbsp;

---

## Station 5 – Environments, saving data, DuckDB, Spark ML  (Session 4 · 10 P)

**5.1 (3 P)** `requirements.txt`, a lock file (`pip freeze` / `uv.lock`) and a dev container: what does each one pin,
and why is the container the strongest guarantee for our Spark labs?

**5.2 (3 P)** (a) Someone posts `best_model.pkl` in a forum. Why should you not load it? (b) Five Python processes want to
**write** into the same `taxi.duckdb` file at the same time. What happens, and why?

**5.3 (4 P)** (a) Transformer or Estimator: `StringIndexer`, `VectorAssembler`, `LinearRegression`.
(b) A `CrossValidator` with a 3 × 2 parameter grid and `numFolds=5`: how many model fits in total?
(c) We predict the trip **duration** at pickup. Why is `fare_amount` not allowed as a feature? Name the problem.

&nbsp;
&nbsp;

---

## Station 6 – Kafka and streaming architectures  (Sessions 3, 5, 6 · 10 P)

**6.1 (3 P)** Define *topic*, *partition* and *offset*. What does Kafka 4 use to manage cluster metadata instead of ZooKeeper?

**6.2 (4 P)** Topic `payments` has 4 partitions, replication factor 3 on 3 brokers.
(a) Group `fraud` runs 6 consumers, group `dashboard` runs 1 consumer. Which partitions does each consumer read?
(b) What is the ISR, and what happens when the leader broker of partition 2 fails?

**6.3 (3 P)** A company computes the same KPIs twice: a nightly Spark batch job **and** a streaming job.
Name this architecture and its main drawback. How does a Kappa architecture with Kafka's append-only log avoid it?

&nbsp;
&nbsp;
