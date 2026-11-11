# Block 3 – Exam circuit · model answers (NOT for the student repo)

Total 58 P. Point breakdown in brackets. Sources: decks of Sessions 2–5 (08.10., 14.10., 28.10., 05.11.2026) and the
Session 4 teacher guide.

---

## Station 1 – Do you need a cluster? (9 P)

**1.1 (2 P)** Volume, Velocity, Variety (1 P for all three). One more V with taxi example (1 P), e.g.
*Veracity*: negative fares / zero-distance trips / dropoff before pickup – data quality; *Variability*: demand peaks on
New Year's Eve, changing schemas between years; *Value*: pricing or fleet planning from the trips.

**1.2 (3 P)** Scale up = bigger machine: + simple code, no network, no partial failures; − hardware ceiling, high-end
servers cost disproportionately more, single point of failure (1 P each side for 1 + 1 out of the lists = 2 P).
Scale out = more machines: + no hard ceiling, fault tolerance, cost efficiency with commodity hardware; − network,
failures, coordination, stragglers. Rule of thumb: **scale up first, scale out when you must** (1 P).

**1.3 (4 P)** (a) **Threads** (or async): I/O-bound, the GIL is released while waiting for the network (2 P).
(b) **Processes** (`multiprocessing`, n_processes = n_cores = 8): CPU-bound Python code; with threads only one thread
executes bytecode at a time because of the GIL; each process has its own interpreter and GIL. Cost: data must be copied
between processes (2 P). Free-threaded Python 3.13+ is a bonus remark, not required.

---

## Station 2 – File formats and engines (9 P)

**2.1 (2 P)** Row: CSV, JSON, Avro · Column: Parquet, ORC (1 P). Kafka events: **Avro** (or JSON Lines) (1 P).

**2.2 (3 P)** Any three, 1 P each: reads only the one needed column (columnar layout, projection); binary + typed –
no text parsing / type guessing; compression (similar values next to each other) → less I/O; min/max statistics per row
group let engines skip blocks; engines read Parquet in parallel.

**2.3 (4 P)** Lazy: the calls only build a query plan; an optimiser rewrites it before anything is read, execution
starts at `.collect()` (1 P). Projection pushdown: read only the columns the query needs (1 P). Predicate pushdown:
apply the filter while reading, skipping rows/row groups via statistics (1 P). pandas is eager: every line runs
immediately and loads the full result, there is no plan and no optimiser that can see the later `select`/`filter` (1 P).

---

## Station 3 – HDFS, MapReduce, Spark architecture (10 P)

**3.1 (3 P)** 150 MB / 64 MB → **3 blocks** (64 + 64 + 22 MB) (1 P); × 3 replicas = **9 block copies** on different
DataNodes (1 P). NameNode: metadata – file names, which blocks form a file, where each block replica lives; DataNodes:
the actual block data, they serve reads/writes, replicate, and send heartbeats to the NameNode (1 P).
(Hadoop's current default block size is 128 MB; the lecture example used 64 MB as in GFS – accept both if the student
calculates consistently.)

**3.2 (3 P)** Map: (big,1) (data,1) (is,1) (big,1) (data,1) (is,1) (fast,1) (1 P).
Shuffle & sort: big → [1,1], data → [1,1], fast → [1], is → [1,1] (1 P).
Reduce: big 2, data 2, fast 1, is 2 (1 P). A combiner could pre-sum (big,2) on the mapper – bonus.

**3.3 (4 P)** Driver: runs `main`/the SparkSession, builds the plan (DAG), splits it into stages and tasks, schedules
them, collects results (1 P). Executors: JVM processes on worker nodes that run tasks and cache data (1 P). Cluster
manager (YARN, Kubernetes, standalone; `local[*]` on Kaggle): allocates resources/executors (1 P).
Action → **job**; the job is cut into **stages** at shuffle boundaries (wide transformations); each stage runs
one **task per partition** (1 P).

---

## Station 4 – Spark execution and joins (10 P)

**4.1 (2 P)** Transformations: `filter`, `groupBy().agg()`, `withColumn`. Actions: `count`, `show`, `write.parquet`.
(6 × ⅓ P, round down.)

**4.2 (5 P)** — the artefact task
(a) Number of trips with a positive distance **per pickup zone (zone name)**, sorted descending (1 P).
(b) **Two shuffles** = two `Exchange` nodes: `hashpartitioning(Zone)` for the `groupBy` aggregation and
`rangepartitioning` for the `orderBy` (sort) (1 P number, 1 P operations). The `BroadcastExchange` is **not** a shuffle of
the big table – it sends the small table to every executor (accept if explained correctly).
(c) The right side, the **zone lookup CSV** (`BuildRight`): 265 rows, far below the 10 MB
`autoBroadcastJoinThreshold` → each task joins its trips locally, the large trip table never moves (1 P).
(d) `PushedFilters`: the filter `trip_distance > 0` is evaluated in the Parquet reader (row groups skipped via statistics);
`ReadSchema`: only 2 of 19 columns are read – projection pushdown (1 P).
Also acceptable: the partial/final `HashAggregate` pair = map-side pre-aggregation before the shuffle (like a combiner).

**4.3 (3 P)** 2 GB / 128 MB ≈ **16 partitions → 16 tasks** (1 P); 4 cores run 4 tasks at a time → **4 waves** (1 P).
Only 2 partitions → only 2 tasks, 2 of the 4 cores idle; fix with `repartition` (1 P).

---

## Station 5 – Environments, saving data, DuckDB, Spark ML (10 P)

**5.1 (3 P)** `requirements.txt`: the direct dependencies, usually version ranges (1 P). Lock file: exact versions of
**all** packages incl. transitive ones (1 P). Container: additionally the OS, the Python interpreter and system software
such as **Java 17** for Spark – the whole runtime (1 P).

**5.2 (3 P)** (a) Unpickling can execute arbitrary code (`__reduce__` → e.g. `os.system`); a pickle also needs the same
library versions to load. Only load your own/trusted pickles; share data as Parquet (2 P).
(b) DuckDB is embedded, single-file: **one writing process** at a time (or many readers). The other processes get a
lock error (`Could not set lock on file`) (1 P). Many writers → client–server DB like PostgreSQL.

**5.3 (4 P)** (a) `StringIndexer` = Estimator (learns the label → index mapping), `VectorAssembler` = Transformer,
`LinearRegression` = Estimator (2 P: all three correct = 2 P, two correct = 1 P).
(b) 3 × 2 × 5 = 30 + 1 refit on all training data = **31** (1 P).
(c) The fare is known only **after** the trip – it depends on the duration we want to predict: **target leakage**;
great test score, useless at pickup time (1 P).

---

## Station 6 – Kafka and streaming architectures (10 P)

**6.1 (3 P)** Topic: named stream of related events (roughly like a table, but append-only, not queryable) (½ P);
partition: ordered, append-only log, a topic is split into partitions for parallelism (1 P); offset: position of a record
inside one partition (½ P). Kafka 4: **KRaft** (Kafka Raft metadata mode) – controllers inside Kafka; ZooKeeper removed (1 P).

**6.2 (4 P)** (a) `fraud`: 4 consumers get one partition each, **2 are idle** (max. parallelism = number of partitions)
(1 P). `dashboard`: its single consumer reads **all 4** partitions – each group gets all messages independently (1 P).
(b) ISR = in-sync replicas: the replicas that are fully caught up with the leader (1 P). Leader of partition 2 fails →
the controller elects a new leader from the ISR, producers and consumers switch to it; with `acks=all` no acknowledged
message is lost (1 P).

**6.3 (3 P)** **Lambda** architecture (1 P). Drawback: the same logic in two code bases (batch + speed layer), higher
complexity and operating cost, results can disagree (1 P). Kappa: only a stream layer; Kafka keeps the immutable,
append-only log long enough, so "batch" recomputation = **replaying the log from offset 0** with the new stream job (1 P).
