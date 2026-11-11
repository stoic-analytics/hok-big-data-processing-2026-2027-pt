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

## Station 4 – Spark execution and joins: read the plan  (Sessions 2, 4 · 10 P)

**4.1 (2 P)** Transformation or action? `filter`, `count`, `groupBy().agg()`, `show`, `withColumn`, `write.parquet`.

**4.2 (5 P)** A colleague ran `.explain()` and sends you this physical plan (shortened):
```
AdaptiveSparkPlan isFinalPlan=false
+- Sort [trips#120L DESC NULLS LAST], true, 0
   +- Exchange rangepartitioning(trips#120L DESC NULLS LAST, 200)
      +- HashAggregate(keys=[Zone#45], functions=[count(1)])
         +- Exchange hashpartitioning(Zone#45, 200)
            +- HashAggregate(keys=[Zone#45], functions=[partial_count(1)])
               +- Project [Zone#45]
                  +- BroadcastHashJoin [PULocationID#7], [LocationID#44], Inner, BuildRight
                     :- Filter (isnotnull(trip_distance#4) AND (trip_distance#4 > 0.0))
                     :  +- FileScan parquet [trip_distance#4,PULocationID#7]
                     :       PushedFilters: [IsNotNull(trip_distance), GreaterThan(trip_distance,0.0)],
                     :       ReadSchema: struct<trip_distance:double,PULocationID:int>
                     +- BroadcastExchange HashedRelationBroadcastMode(...)
                        +- FileScan csv [LocationID#44,Zone#45]
```
(a) Describe in one sentence what the query computes. (1 P)
(b) How many **shuffles** does the plan contain, and which operations cause them? (2 P)
(c) Which table is broadcast, and why is that a good choice here? (1 P)
(d) What do `PushedFilters` and `ReadSchema` tell you about how much data Spark reads? (1 P)

**4.3 (3 P)** A 2 GB CSV file, `spark.sql.files.maxPartitionBytes` = 128 MB, a machine with 4 cores.
About how many partitions and tasks, and how many "waves" of tasks? What happens if the data had only 2 partitions?

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
