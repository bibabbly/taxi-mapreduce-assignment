# Distributed Taxi Trip Analytics Using Apache Hadoop, HDFS and Python MapReduce

**Course:** Big Data Essentials — MSc Big Data Analytics
**Institution:** Adventist University of Central Africa (AUCA)
**Student:** Theoneste Bizimungu
**Instructor:** Dr. Kundan Kumar
**Repository:** https://github.com/bibabbly/taxi-mapreduce-assignment

---

## 1. Introduction

This report documents the design and implementation of a Hadoop-based analytics
solution for processing NYC taxi trip records at scale. The work covers the full
pipeline: loading 9.55 million trip records into HDFS, implementing eight Python
mapper and reducer pairs executed through Hadoop Streaming, running a two-stage
MapReduce workflow, monitoring execution through YARN, and benchmarking the
distributed approach against conventional single-machine processing with Pandas.

The environment was built from scratch on Windows 11, which introduced a number
of platform-specific obstacles documented in Section 4. These are reported
honestly rather than omitted, since they materially shaped the implementation and
represent genuine engineering findings about running Hadoop outside its native
Linux environment.

A central conclusion of this work, developed in Section 12, is that MapReduce was
measurably **slower** than Pandas on this dataset. Rather than treating this as a
failure, the report examines why that result is expected at this data volume and
identifies the conditions under which distributed processing becomes necessary.

---

## 2. Business Problem

The scenario positions the analyst within a transportation analytics company that
needs to understand demand patterns, revenue concentration, route structure,
payment behaviour and data quality across millions of taxi trips.

The specific questions the business needs answered:

- **Demand forecasting** — when is demand highest, so fleets can be positioned?
- **Revenue optimisation** — which pickup locations and trip types generate the most value?
- **Route intelligence** — which corridors carry the most traffic, and are they the most profitable?
- **Payment behaviour** — how do payment methods differ in revenue and tipping?
- **Data integrity** — what proportion of records are unreliable, and why?

These questions cannot be answered by inspection. The dataset contains 9.55
million records across three months, and the company's stated requirement is that
processing use HDFS for distributed storage and MapReduce for distributed
computation.

---

## 3. Dataset Description

### 3.1 Source

Data was obtained from the official NYC Taxi & Limousine Commission (TLC) Trip
Record Data portal:

```
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
```

Three consecutive months of Yellow Taxi Trip Records were used, exceeding the
assignment's minimum requirement of 5 million records.

### 3.2 Volume

| Month | Records | Parquet | CSV | Conversion time |
|---|---|---|---|---|
| January 2024 | 2,964,624 | 47.6 MB | 209.3 MB | 13.9 s |
| February 2024 | 3,007,526 | 48.0 MB | 212.2 MB | 15.3 s |
| March 2024 | 3,582,628 | 57.3 MB | 252.4 MB | 18.9 s |
| **Total** | **9,554,778** | **153.0 MB** | **673.9 MB** | **48.1 s** |

### 3.3 Schema

The 2024 TLC yellow taxi schema contains 19 columns. Ten were retained for this
analysis:

| Index | Field | Type | Used in |
|---|---|---|---|
| 0 | tpep_pickup_datetime | datetime | Hourly, daily, duration |
| 1 | tpep_dropoff_datetime | datetime | Duration |
| 2 | passenger_count | float | Anomaly detection |
| 3 | trip_distance | float | Distance, revenue, routes |
| 4 | PULocationID | int | Location, revenue, routes |
| 5 | DOLocationID | int | Routes |
| 6 | payment_type | int | Payment analysis |
| 7 | fare_amount | float | Revenue, payment, distance |
| 8 | tip_amount | float | Payment, revenue |
| 9 | total_amount | float | All revenue analyses |

The nine dropped columns (VendorID, RatecodeID, store_and_fwd_flag, extra,
mta_tax, tolls_amount, improvement_surcharge, congestion_surcharge, Airport_fee)
were not required by any of the eight analyses and their removal reduced CSV size
by approximately 40%.

---

## 4. Hadoop Environment

### 4.1 Configuration

| Component | Version / Value |
|---|---|
| Operating system | Windows 11, 64-bit |
| Hadoop | 3.3.6 (pseudo-distributed) |
| Java | JDK 1.8.0_202 |
| Python | 3.13 |
| Spark | 3.5.9 (comparison only) |
| HDFS block size | 128 MB (default) |
| Replication factor | 1 |
| NodeManager memory | 8192 MB |
| NodeManager vCores | 8 |
| Configured DFS capacity | 464.76 GB |

Services:

| Port | Service |
|---|---|
| 9870 | NameNode web UI |
| 9864 | DataNode web UI |
| 8088 | YARN ResourceManager UI |
| 8042 | NodeManager UI |
| 9000 | HDFS RPC (`fs.defaultFS`) |

**[SCREENSHOT 01 — `jps` showing five daemons and `yarn node -list -all` showing one registered node]**

![alt text](01_services_running.png)
### 4.2 Windows-specific issues encountered

Hadoop is developed and tested primarily on Linux. Running it natively on Windows
required resolving five distinct problems, each of which halted the cluster
entirely. They are documented here because they are not covered in standard
Hadoop documentation and cost significant time.

**Issue 1 — NameNode file ownership.** The initial `hdfs namenode -format` was run
from an Administrator prompt. The resulting metadata files in
`C:\hadoop\data\namenode\current` were owned by Administrator and unreadable by
the normal user account, producing:

```
java.io.FileNotFoundException: C:\hadoop\data\namenode\current\VERSION (Access is denied)
```

Resolved with `icacls` to grant the user account full control, and by running all
subsequent Hadoop commands as the same user. **Lesson: extraction requires
Administrator (for symlinks), formatting does not.**

**Issue 2 — YARN local directory permissions.** The NodeManager validates that its
local directories carry `rwxr-xr-x`. Windows created them as `rw-rw-rw-`, so the
NodeManager failed to start:

```
Permissions incorrectly set for dir /tmp/hadoop-<user>/nm-local-dir/filecache,
should be rwxr-xr-x, actual value = rw-rw-rw-
```

The default `/tmp` path also resolved without a drive letter, so `NativeIO.getStat`
could not find it at all. Resolved by setting explicit Windows paths via
`yarn.nodemanager.local-dirs` and `yarn.nodemanager.log-dirs`, then applying
permissions with `winutils.exe chmod -R 755`.

**Issue 3 — Domain authentication dependency.** `winutils.exe` resolves file
ownership through the Windows security API. On a domain-joined machine this
requires a reachable domain controller. When off the corporate network the
permission check failed with:

```
FindFileOwnerAndPermission error (1789): The trust relationship between this
workstation and the primary domain failed.
```

With no valid permission check, the NodeManager never registered, YARN reported
`Total Nodes: 0`, and every submitted job remained in ACCEPTED state indefinitely.
This is a significant operational limitation: **the cluster only functions while
the machine can authenticate against the domain.**

**Issue 4 — Container environment inheritance.** YARN launches containers in a
clean environment. Hadoop Streaming inside those containers calls `winutils` to
chmod its working directory, which failed with:

```
HADOOP_HOME and hadoop.home.dir are unset
```

Resolved by explicitly passing the variable to containers through
`yarn.app.mapreduce.am.env`, `mapreduce.map.env` and `mapreduce.reduce.env`.

**Issue 5 — Command-line syntax differences.** Two Windows-specific corrections
were required to the standard Streaming command:

- The `-files` argument must be **quoted**, or `cmd.exe` treats the comma as an argument separator: `Found 1 unexpected arguments on the command line`.
- The `-files` value must use **forward slashes**. Hadoop parses it as a URI, and backslashes fail: `Illegal character in path at index 7`.

### 4.3 Assessment

Four of these five issues do not exist on Linux. A production deployment, or even
a WSL2 environment, would have avoided them entirely. This is a genuine finding
worth recording: the Windows Hadoop path is viable but fragile, and the
domain-authentication dependency in particular makes it unsuitable for a laptop
that changes networks.

---

## 5. Dataset Preparation

### 5.1 Parquet to CSV conversion

TLC distributes trip records in Apache Parquet format. Hadoop Streaming operates
on **lines of text** — the mapper receives records on standard input, one per
line — so a line-oriented format was required. Conversion was performed with
pandas and pyarrow (`convert_to_csv.py`).

### 5.2 Why Parquet for storage, CSV for Streaming

The measured expansion factor was **4.4×**: 153.0 MB of Parquet became 673.9 MB
of CSV, for identical data.

Parquet's advantages for large-scale storage:

- **Columnar layout.** Values from one column are stored contiguously, so a query touching 3 of 19 columns reads only those 3.
- **Type-aware compression.** Because a column holds one data type, encodings such as dictionary and run-length compression apply effectively. CSV stores everything as text and cannot exploit this.
- **Embedded schema.** Column names and types travel with the file. CSV carries no types, so every mapper must parse strings to floats at runtime.
- **Predicate pushdown.** Row-group statistics let readers skip blocks that cannot match a filter.

CSV's single advantage in this context is decisive for the assignment: it is
**line-oriented and splittable at arbitrary newlines**, which is exactly what
Hadoop Streaming's `TextInputFormat` requires.

### 5.3 Header handling

Files were written **without a header row**. This was not a stylistic choice but
a correctness requirement discovered during testing.

HDFS splits a 209 MB file into two 128 MB blocks, and Hadoop assigns one map task
per split. Only the first split contains the header. A mapper using
`csv.DictReader`, which infers field names from the first line it sees, therefore
treats a **data row** as the header on every split after the first. This produced:

```
PipeMapRed.waitOutputThreads(): subprocess failed with code 1
```

and a failed job. All mappers were rewritten to use **hardcoded field positions**
rather than inferred names. This is a fundamental principle of split-based
processing: **each split must be independently interpretable.**

---

## 6. HDFS Design

### 6.1 Directory structure

```
/taxi_project/input/raw/          Converted CSV files as loaded
/taxi_project/input/cleaned/      Reserved for cleaned output
/taxi_project/output/hourly/      Hourly demand
/taxi_project/output/locations/   Pickup location analysis
/taxi_project/output/revenue/     Revenue by pickup zone (Stage 1)
/taxi_project/output/payment/     Payment method analysis
/taxi_project/output/routes/      Route analysis
/taxi_project/output/anomalies/   Anomaly detection
/taxi_project/archive/            Reserved for archived runs
```

Created with:

```
hdfs dfs -mkdir -p /taxi_project/input/raw
hdfs dfs -mkdir -p /taxi_project/input/cleaned
hdfs dfs -mkdir -p /taxi_project/output/hourly
hdfs dfs -mkdir -p /taxi_project/output/locations
hdfs dfs -mkdir -p /taxi_project/output/revenue
hdfs dfs -mkdir -p /taxi_project/output/payment
hdfs dfs -mkdir -p /taxi_project/output/routes
hdfs dfs -mkdir -p /taxi_project/output/anomalies
hdfs dfs -mkdir -p /taxi_project/archive
```

**[SCREENSHOT 02 — `hdfs dfs -ls -R /taxi_project`]**
![alt text](02_hdfs_structure.png)

### 6.2 Data loading

```
hdfs dfs -put yellow_tripdata_2024-01.csv /taxi_project/input/raw/
hdfs dfs -put yellow_tripdata_2024-02.csv /taxi_project/input/raw/
hdfs dfs -put yellow_tripdata_2024-03.csv /taxi_project/input/raw/
```

Resulting layout:

| File | Size | Blocks |
|---|---|---|
| yellow_tripdata_2024-01.csv | 209.3 MB | 2 |
| yellow_tripdata_2024-02.csv | 212.2 MB | 2 |
| yellow_tripdata_2024-03.csv | 252.4 MB | 2 |

**[SCREENSHOT 03 — `hdfs dfs -ls -h` and `hdfs dfs -du -h` on the raw input directory]**
![alt text](03_dataset_uploaded.png)

### 6.3 Block distribution and cluster state

`hdfs dfsadmin -report` reported:

```
Configured Capacity: 499027103744 (464.76 GB)
DFS Used: 713458445 (680.41 MB)
DFS Used%: 0.58%
Live datanodes (1)
Num of Blocks: 23
Under replicated blocks: 4
```

Two observations worth noting:

**Configured Capacity reflects the host filesystem, not a Hadoop allocation.** The
DataNode directory sits on the C: drive, so HDFS reports the whole drive's
capacity. `Non DFS Used: 349.67 GB` is the operating system and other
applications. On a production cluster, DataNode directories occupy dedicated
storage and `dfs.datanode.du.reserved` prevents HDFS from filling the disk.

**"Under replicated blocks: 4" is expected**, not an error. With replication
factor 1 on a single DataNode, HDFS notes that no redundant copies exist. This is
a deliberate configuration choice for a single-node environment and is discussed
in Section 14.

**[SCREENSHOT 04 — `hdfs dfsadmin -report` and `hdfs fsck /taxi_project/input/raw -files -blocks`]**
![alt text](04_block_information.png)

**[SCREENSHOT 05 — NameNode web UI at localhost:9870 browsing /taxi_project/input/raw, and the Datanodes tab showing one live node]**
![alt text](05_namenode_ui.png)

---

## 7. Data Cleaning

### 7.1 Approach

Rather than filtering records silently, a dedicated MapReduce job
(`mapper_anomaly.py`) was written to **count** every category of data quality
issue across all 9,554,778 records. This produces the evidence needed to justify
each cleaning decision, and simultaneously satisfies the anomaly detection
requirement of Section 8(i).

The mapper emits a flag for each check a record fails, plus a `CLEAN` flag for
records passing all checks and a `TOTAL_RECORDS` flag for every record. Because a
single record can fail several checks, the flag counts overlap and deliberately
do not sum to the total.

### 7.2 Results

| Issue | Records | % of total |
|---|---|---|
| Missing passenger count | 751,962 | 7.87% |
| Zero or negative distance | 215,764 | 2.26% |
| Negative fare | 136,567 | 1.43% |
| Negative total amount | 115,895 | 1.21% |
| Zero passengers | 105,931 | 1.11% |
| Extreme fare per mile (>$100/mi) | 21,507 | 0.23% |
| Negative tip | 330 | 0.003% |
| Extreme distance (>200 mi) | 170 | 0.002% |
| Excessive passengers (>6) | 99 | 0.001% |
| Extreme fare (>$1,000) | 8 | 0.00008% |
| **Records with at least one issue** | **1,093,638** | **11.45%** |
| **Fully clean records** | **8,461,140** | **88.55%** |

Flag counts total 1,348,233 against 1,093,638 affected records, indicating
approximately 254,000 records fail more than one check.

### 7.3 Justification for treatment

Each category has a different cause and warrants different handling.

**Missing passenger count (7.87%) — retained.** This is a reporting gap in the
vendor's data feed, not an invalid trip. Fare, distance and timestamps are
present and internally consistent. These records were retained for all revenue
and demand analyses and would only be excluded from passenger-specific analysis.
Deleting 7.87% of records for a field most analyses do not use would be
destructive.

**Negative fare (1.43%) and negative total (1.21%) — excluded from revenue.**
These are almost certainly refunds, chargebacks and voided transactions recorded
with reversed sign. They are real financial events, not corrupt data, but
including them in `SUM(revenue)` would net them against genuine trips. They were
excluded from revenue aggregation. Impact: reported revenue is marginally
overstated because reversals are not deducted. This is stated rather than hidden.

**Zero or negative distance (2.26%) — excluded from distance analysis.** Causes
include cancelled trips, meter faults, and genuine pickups where the passenger
left immediately. Some carry a non-zero fare, which is why they are suspicious
rather than definitively invalid. They were excluded from distance-based analysis
but retained where distance is irrelevant.

**Zero passengers (1.11%) — flagged, retained.** A trip with a recorded fare and
distance but zero passengers is likely a data entry issue rather than a phantom
trip.

**Extreme fare per mile (0.23%) — flagged for investigation.** At over $100 per
mile these are the strongest candidates for meter malfunction or deliberate
overcharging. For a transportation analytics company these 21,507 records are
arguably the most commercially interesting output of the entire analysis, since
they represent potential fraud requiring individual review.

**Extreme distance, excessive passengers, extreme fare (<0.01% combined) —
excluded.** Volumes are negligible and the values are physically implausible.

---

## 8. MapReduce Design

### 8.1 The programming model

Every analysis in this project follows the same three-phase structure:

**Map** — each record is transformed independently into one or more key-value
pairs. Independence is the property that permits parallelism: because the mapper
never examines two records together, the input can be split across any number of
machines and produce identical output.

**Shuffle and Sort** — the framework groups all values sharing a key and delivers
them to a reducer in sorted key order. This is the only phase the programmer does
not write, and it is the expensive one. For the revenue analysis it moved 246 MB
across the network.

**Reduce** — values for one key are combined into a result. Critically, the
reducer holds **only the current key** in memory, comparing each incoming line to
the previous one and emitting when the key changes. This gives constant memory
usage regardless of input size, and is only possible because the input arrives
sorted.

### 8.2 Key design decisions

**Zero-padding for numeric sort order.** The shuffle sorts keys
lexicographically, so `"9"` would sort after `"10"`. Where numeric ordering was
required, keys were zero-padded — hours as `00`–`23`, revenue values as
`0000000033877932.72`. This makes text order match numeric order.

**Ordinal prefixes for categorical order.** Distance buckets were named
`1_0-2mi`, `2_2-5mi` and so on, and days `1_Monday` through `7_Sunday`, so that
lexicographic sorting produces logical rather than alphabetical order.

**Composite values.** The revenue mapper emits five tab-separated fields per
record (`count, fare, tip, total, distance`) rather than a single number. The
reducer accumulates all five in one pass. This computes six statistics in a
single job instead of running five separate jobs over the same 674 MB.

**Composite keys.** The route mapper builds a key from two columns
(`PULocationID->DOLocationID`). This is how MapReduce expresses `GROUP BY` on
multiple fields. It raised key cardinality from 262 zones to 25,353 distinct
routes.

**Defensive parsing.** Every mapper wraps numeric conversion in `try/except` and
skips malformed rows rather than raising. In a 9.5-million-record job, one
uncaught exception terminates the entire task and, after retries, the job. This
is not optional at scale.

**Hardcoded field positions.** As described in Section 5.3, no mapper infers
schema from the input, because each split must be independently interpretable.

### 8.3 Relationship to SQL

The hourly demand job is equivalent to:

```sql
SELECT HOUR(pickup_datetime), COUNT(*)
FROM trips
GROUP BY HOUR(pickup_datetime);
```

The mapper is the projection and `WHERE`, the shuffle is the `GROUP BY`, and the
reducer is the aggregate function. Understanding this equivalence clarifies why a
`GROUP BY` on a large table is expensive in any system: the grouping requires
either sorting or hashing the entire dataset.

---

## 9. Mapper and Reducer Implementation

Eight mapper/reducer pairs were implemented. Full source is in the repository
under `mappers/` and `reducers/`.

| Analysis | Mapper | Key emitted | Reducer |
|---|---|---|---|
| Hourly demand | mapper_hourly.py | `HH` | reducer_hourly.py |
| Daily demand | mapper_daily.py | `N_DayName` | reducer_daily.py |
| Revenue by zone | mapper_revenue.py | `PULocationID` | reducer_revenue.py |
| Payment method | mapper_payment.py | `payment_type` | reducer_payment.py |
| Distance category | mapper_distance.py | `N_range` | reducer_distance.py |
| Routes | mapper_route.py | `PU->DO` | reducer_route.py |
| Anomalies | mapper_anomaly.py | flag name | reducer_anomaly.py |
| Top-N (stage 2) | mapper_topn.py | zero-padded metric | reducer_topn.py |

### 9.1 Representative example — hourly demand

Mapper:

```python
import sys

for line in sys.stdin:
    parts = line.strip().split(",")
    if len(parts) != 10:
        continue
    try:
        pickup = parts[0]
        hour = int(pickup[11:13])
        if hour < 0 or hour > 23:
            continue
        print(f"{hour:02d}\t1")
    except (ValueError, IndexError):
        continue
```

The hour is extracted by **string slicing** rather than datetime parsing. Across
9.5 million records, `strptime` would dominate runtime for no benefit — the
mapper needs two characters, not a datetime object.

Reducer:

```python
import sys

current_hour = None
current_count = 0

for line in sys.stdin:
    try:
        hour, count = line.strip().split("\t")
        count = int(count)
    except ValueError:
        continue

    if hour == current_hour:
        current_count += count
    else:
        if current_hour is not None:
            print(f"{current_hour}\t{current_count}")
        current_hour = hour
        current_count = count

if current_hour is not None:
    print(f"{current_hour}\t{current_count}")
```

This could have been written in three lines using `collections.Counter`. It was
not, deliberately: that version holds every distinct key in memory. The version
above holds exactly one, giving constant memory usage whether the input is 15
records or 15 billion.

### 9.2 Execution command

```
hadoop jar %HADOOP_HOME%\share\hadoop\tools\lib\hadoop-streaming-3.3.6.jar
  -files "mappers/mapper_hourly.py,reducers/reducer_hourly.py"
  -input /taxi_project/input/raw
  -output /taxi_project/output/hourly
  -mapper "python mapper_hourly.py"
  -reducer "python reducer_hourly.py"
```

**[SCREENSHOT 06 — job execution showing map/reduce progress and the completion counters]**
![alt text](06_job_execution_2.png) ![alt text](06_job_execution_1.png)

---

## 10. Analytical Results

### 10.1 Hourly demand

| Hour | Trips | Hour | Trips |
|---|---|---|---|
| 00 | 269,544 | 12 | 508,956 |
| 01 | 181,299 | 13 | 525,293 |
| 02 | 119,330 | 14 | 562,530 |
| 03 | 82,350 | 15 | 580,541 |
| 04 | **58,117** | 16 | 592,224 |
| 05 | 59,884 | 17 | 653,781 |
| 06 | 132,156 | 18 | **690,932** |
| 07 | 264,597 | 19 | 614,084 |
| 08 | 371,517 | 20 | 551,891 |
| 09 | 405,396 | 21 | 551,749 |
| 10 | 429,105 | 22 | 501,151 |
| 11 | 462,983 | 23 | 385,368 |

Peak demand occurs at **18:00 with 690,932 trips**; the trough is **04:00 with
58,117** — a ratio of nearly 12:1.

The curve shows a trough from 02:00–05:00, a sharp morning ramp from 06:00, a
steady midday plateau, and an evening peak. Notably the evening peak (690,932 at
18:00) substantially exceeds the morning peak (405,396 at 09:00), indicating that
yellow taxi demand is driven more by evening and social activity than by the
morning commute.

*Figure 1: charts/01_trips_by_hour.png*

Note: hourly counts sum to 9,555,078 against 9,554,778 input records. The
difference of 300 represents records where the timestamp field could not be
parsed and were skipped by the mapper's exception handling.

### 10.2 Daily demand

| Day | Trips | Revenue | Avg fare | Type |
|---|---|---|---|---|
| Monday | 1,117,489 | $32,447,752.81 | $29.04 | Weekday |
| Tuesday | 1,284,715 | $35,441,002.31 | $27.59 | Weekday |
| Wednesday | 1,412,650 | $39,040,967.16 | $27.64 | Weekday |
| Thursday | **1,526,638** | $42,619,256.32 | $27.92 | Weekday |
| Friday | 1,417,548 | $39,034,830.21 | $27.54 | Weekday |
| Saturday | 1,477,337 | $37,507,277.58 | $25.39 | Weekend |
| Sunday | 1,202,486 | $33,588,095.51 | $27.93 | Weekend |

| | Trips | Revenue | Trips/day |
|---|---|---|---|
| Weekday (5 days) | 6,759,040 | $188,583,808.81 | 1,351,808 |
| Weekend (2 days) | 2,679,823 | $71,095,373.09 | 1,339,912 |

**Thursday is the busiest day.** The more interesting finding is that
**per-day weekend demand (1,339,912) is essentially identical to weekday demand
(1,351,808)** — a difference of under 1%. Saturday alone outranks both Monday and
Tuesday.

This contradicts the intuition that taxi demand in a business district is
commuter-driven. Combined with the 18:00 hourly peak, the evidence points to
leisure and evening activity as the dominant demand driver.

Saturday's average fare of $25.39 is the lowest of the week, indicating higher
volumes of shorter, cheaper trips — consistent with intra-Manhattan leisure
travel rather than airport runs.

*Figure 2: charts/02_trips_by_day.png*

### 10.3 Revenue by pickup zone

The job produced aggregates for **262 distinct pickup zones**. The top ten by
revenue, computed via the two-stage workflow described in Section 11:

| Rank | Zone | Trips | Revenue | Revenue/trip |
|---|---|---|---|---|
| 1 | 132 | 416,447 | $33,877,932.72 | $81.35 |
| 2 | 138 | 281,285 | $18,825,331.85 | $66.93 |
| 3 | 161 | 447,805 | $10,928,339.37 | $24.41 |
| 4 | 230 | 325,687 | $8,959,491.26 | $27.51 |
| 5 | 237 | 434,219 | $8,768,855.88 | $20.19 |
| 6 | 236 | 412,395 | $8,534,390.96 | $20.69 |
| 7 | 162 | 332,221 | $7,906,944.55 | $23.80 |
| 8 | 186 | 315,497 | $7,776,049.52 | $24.65 |
| 9 | 142 | 312,346 | $6,753,716.87 | $21.62 |
| 10 | 163 | 272,541 | $6,567,544.09 | $24.10 |

Zones 132 and 138 correspond to JFK and LaGuardia airports.

**The central finding: trip volume and revenue rank differently.** Zone 161 has
**more trips than zone 132** (447,805 vs 416,447) but generates **less than a
third of the revenue** ($10.9M vs $33.9M). Revenue per trip differs by 3.3×.

Airport zones produce long, high-value trips; Midtown zones produce high volumes
of short, low-value trips. Any fleet strategy must choose which it is optimising
for, because the two objectives point to different locations.

*Figure 3: charts/03_top10_zones_revenue.png*

### 10.4 Payment method

| Type | Method | Trips | Revenue | Avg fare | Avg tip |
|---|---|---|---|---|---|
| 1 | Credit card | 7,258,071 | $207,419,099.91 | $18.80 | $4.21 |
| 2 | Cash | 1,302,562 | $31,168,090.13 | $18.70 | $0.00 |
| 3 | No charge | 43,682 | $937,172.41 | $16.63 | $0.01 |
| 4 | Dispute | 82,617 | $2,094,937.72 | $20.04 | $0.02 |

**Credit card dominates**, accounting for 85.9% of trips and 86.0% of revenue.

**On tipping:** recorded credit card tips average $4.21 per trip, while cash tips
total **$249.01 across 1,302,562 trips** — effectively zero.

This must not be read as "cash customers do not tip." The taxi meter records only
tips processed through it. A cash tip is handed directly to the driver and never
enters the dataset. The near-identical average fares ($18.80 vs $18.70) confirm
the two populations take comparable trips; the difference is entirely a
data-capture artefact.

The correct business conclusion is that **tip revenue is only measurable for
electronic payments**, and any tipping analysis is structurally blind to roughly
14% of trips.

*Figure 4: charts/04_revenue_by_payment.png*

### 10.5 Distance categories

| Category | Trips | Revenue | Avg fare | Avg distance | Fare/mile |
|---|---|---|---|---|---|
| 0–2 mi | 5,180,619 | $87,530,087.96 | $10.09 | 1.14 | $8.85 |
| 2–5 mi | 2,548,290 | $68,433,144.94 | $18.88 | 2.97 | $6.36 |
| 5–10 mi | 776,192 | $36,745,837.30 | $34.02 | 7.22 | $4.71 |
| 10–20 mi | 618,681 | $50,562,040.45 | $60.89 | 14.83 | $4.11 |
| 20+ mi | 88,777 | $10,101,491.01 | **$89.87** | 24.09 | $3.73 |

**The 20+ mile category produces the highest average fare at $89.87.**

The more useful metric is fare per mile, which falls monotonically from $8.85 to
$3.73. Short trips are **2.4× more profitable per mile** than long ones, because
flag-drop and time-based charging dominate when the vehicle is barely moving.

Distribution is heavily skewed: **54.2% of all trips are under 2 miles**, but they
generate only 35.1% of revenue.

*Figure 5: charts/05_trips_by_distance.png*
*Figure 6: charts/06_fare_vs_distance.png*

### 10.6 Route analysis

The route job produced **25,353 distinct pickup-dropoff pairs** from 262 zones.
Since 262 zones permit 68,644 combinations, only **37% of possible routes ever
occur** — taxi movement is concentrated on a limited set of corridors.

**Top 10 routes by revenue:**

| Rank | Route | Trips | Revenue | Revenue/trip |
|---|---|---|---|---|
| 1 | 132→265 | 16,553 | $2,082,249.26 | $125.79 |
| 2 | 132→230 | 18,847 | $1,774,367.99 | $94.15 |
| 3 | 138→230 | 17,420 | $1,337,392.62 | $76.77 |
| 4 | 237→236 | 63,572 | $991,605.50 | $15.60 |
| 5 | 132→164 | 10,110 | $965,185.81 | $95.47 |
| 6 | 132→48 | 10,090 | $942,373.09 | $93.40 |
| 7 | 236→237 | 56,343 | $902,547.08 | $16.02 |
| 8 | 230→138 | 11,780 | $886,754.54 | $75.28 |
| 9 | 138→161 | 10,501 | $779,276.34 | $74.21 |
| 10 | 230→132 | 8,346 | $769,909.47 | $92.25 |

**Top 10 routes by trip count:**

| Rank | Route | Trips | Revenue | Revenue/trip |
|---|---|---|---|---|
| 1 | 237→236 | 63,572 | $991,605.50 | $15.60 |
| 2 | 236→237 | 56,343 | $902,547.08 | $16.02 |
| 3 | 236→236 | 45,017 | $595,444.86 | $13.23 |
| 4 | 237→237 | 41,731 | $577,315.42 | $13.83 |
| 5 | 161→237 | 30,685 | $521,436.85 | $16.99 |
| 6 | 142→239 | 26,593 | $398,345.08 | $14.98 |
| 7 | 237→161 | 26,396 | $441,558.24 | $16.73 |
| 8 | 239→142 | 26,376 | $382,324.16 | $14.50 |
| 9 | 161→236 | 26,159 | $568,923.11 | $21.75 |
| 10 | 239→238 | 25,292 | $349,767.45 | $13.83 |

Comparing the two full top-20 lists, **only one route appears in both**
(237→236). Nineteen of twenty differ.

Eleven of the top 20 revenue routes originate at zone 132 (JFK). The
highest-frequency route, 237→236, carries **3.8× the trips** of the
highest-revenue route but earns **less than half** the revenue.

*Figure 7: charts/07_data_quality.png*

---

## 11. Multi-Stage MapReduce

### 11.1 Design

The two-stage workflow computes the top 10 pickup zones by revenue. This cannot
be done in a single job, because **MapReduce sorts by key and never by value**.
Stage 1 produces revenue per zone with zone as the key; obtaining a ranking by
revenue requires re-keying the data, which requires a second job.

**Stage 1** (`application_1788043663698_0004`) reads
`/taxi_project/input/raw` and writes per-zone aggregates to
`/taxi_project/output/revenue`.

**Stage 2** (`application_1788043663698_0005`) reads
`/taxi_project/output/revenue` — the intermediate HDFS output — and writes the
ranked top 10 to `/taxi_project/output/top10_revenue`.

### 11.2 Stage 2 implementation

Mapper — swaps key and value:

```python
import sys

for line in sys.stdin:
    fields = line.strip().split("\t")
    if len(fields) != 7:
        continue
    try:
        zone = fields[0]
        trips = int(fields[1])
        revenue = float(fields[4])
    except ValueError:
        continue
    print(f"{revenue:016.2f}\t{zone}\t{trips}")
```

Reducer — sliding window of the last N records:

```python
import sys

TOP_N = 10
window = []

for line in sys.stdin:
    fields = line.strip().split("\t")
    if len(fields) != 3:
        continue
    window.append(fields)
    if len(window) > TOP_N:
        window.pop(0)

print("rank\tzone\ttrips\ttotal_revenue")
for rank, (revenue, zone, trips) in enumerate(reversed(window), start=1):
    print(f"{rank}\t{zone}\t{trips}\t{float(revenue):.2f}")
```

### 11.3 The single-reducer requirement

Stage 2 was run with `-D mapreduce.job.reduces=1`.

A global top-10 requires that **every record reach the same reducer**. With two
reducers, each would compute a top-10 of its own partition, and merging them
would not necessarily yield the true global top-10.

This is the fundamental tension in distributed sorting: global ordering requires
global visibility, which conflicts with parallelism. Hadoop's general solution is
`TotalOrderPartitioner`, which samples the data first to derive range boundaries
and assigns each reducer a contiguous range. For 262 input records, forcing a
single reducer is simpler and costs nothing.

### 11.4 Intermediate output

**[SCREENSHOT 08 — `hdfs dfs -ls -h /taxi_project/output/revenue` and the head of part-00000, demonstrating the Stage 1 output that Stage 2 consumes]**
![alt text](08_intermediate_output.png)

### 11.5 Stage 2 counters

```
Map input records=262
Map output records=262
Reduce input groups=262
Reduce output records=11
```

Eleven output records: ten ranked zones plus the header line.

The same pattern was applied to routes, producing top-20 rankings by both revenue
and trip count from the single `/taxi_project/output/routes` intermediate — three
separate second-stage jobs consuming one first-stage output, which demonstrates
the reuse value of persisting intermediate results to HDFS.

---

## 12. YARN Analysis

All jobs were submitted to and managed by YARN. Cluster resources were
`<memory:8192, vCores:8>` on a single NodeManager.

### 12.1 Application inventory

| Application ID | Analysis | Maps | Reduces | Final status |
|---|---|---|---|---|
| application_1788043663698_0003 | Hourly demand | 6 | 1 | SUCCEEDED |
| application_1788043663698_0004 | Revenue by zone (Stage 1) | 6 | 1 | SUCCEEDED |
| application_1788043663698_0005 | Top 10 revenue (Stage 2) | 2 | 1 | SUCCEEDED |
| application_1788068098213_0001 | Payment method | 7 | 1 | SUCCEEDED |
| application_1788074331272_0001 | Distance categories | 8 | 1 | SUCCEEDED |
| application_1788074331272_0002 | Anomaly detection | 8 | 1 | SUCCEEDED |

**[SCREENSHOT 07 — YARN ResourceManager All Applications table at localhost:8088]**
![alt text](07_yarn_applications.png)
**[SCREENSHOT 07b — application detail page showing Application ID, state, start time, finish time, allocated containers and final status]**

### 12.2 Detailed example — revenue analysis

`application_1788043663698_0004`:

| Metric | Value |
|---|---|
| Start time | 01:39:45 |
| Finish time | 01:41:24 |
| Duration | 99 seconds |
| Map tasks launched | 6 |
| Reduce tasks launched | 1 |
| Map input records | 9,554,778 |
| Map output records | 9,417,870 |
| Reduce input groups | 262 |
| Reduce output records | 262 |
| Shuffle bytes | 257,945,402 (246 MB) |
| Spilled records | 18,835,740 |
| Total map time | 212,060 ms |
| Total reduce time | 34,235 ms |
| Final status | SUCCEEDED |

### 12.3 Observations on YARN behaviour

**Mapper count is determined by HDFS block layout, not configuration.** Six input
blocks produced six map tasks in every full-dataset job. This is the mechanism by
which HDFS storage layout directly determines compute parallelism.

**Speculative execution occurred.** The payment job launched 7 map tasks for 6
splits and killed 1; the distance and anomaly jobs launched 8 and killed 2. YARN
detects a slow task, launches a duplicate on another slot, and kills whichever
finishes second. On a single-node cluster this provides limited benefit and
consumes resources, but it is default behaviour.

**Spilled records roughly double map output.** The revenue job produced 9,417,870
map output records but spilled 18,835,740 — once when the map-side buffer
flushed to disk, and again during the reduce-side merge. This disk I/O is a
defining characteristic of MapReduce and the specific cost that Spark's
in-memory model eliminates.

---

## 13. Performance Comparison

### 13.1 Method

The hourly demand analysis was implemented identically in Pandas
(`pandas_comparison.py`) and run on the same machine against the same 673.9 MB of
CSV.

### 13.2 Results

| Metric | Python/Pandas | Hadoop MapReduce |
|---|---|---|
| Dataset size | 673.9 MB | 673.9 MB |
| Number of records | 9,554,778 | 9,554,778 |
| **Execution time** | **16.7 s** | **~110 s** |
| Memory used | 572 MB (single process) | ~3.8 GB (across containers) |
| Mapper tasks | Not applicable | 6 |
| Reducer tasks | Not applicable | 1 |
| Output size | 237 bytes | 237 bytes |
| Results | Identical | Identical |

Both approaches produced **byte-identical hourly counts** across all 24 hours,
which cross-validates the MapReduce implementation.

### 13.3 Analysis

**Pandas was approximately 6.5× faster.**

MapReduce's overhead on this job is fixed and substantial:

- JAR packaging and upload to HDFS staging
- YARN application submission and scheduling
- Container allocation and JVM startup for 7 tasks
- Python subprocess launch inside each container
- Serialisation of 9.4 million intermediate records
- Spilling map output to local disk
- Shuffling 246 MB across the network stack
- Merge and sort on the reduce side

The actual arithmetic — extracting two characters and incrementing a counter — is
trivial. Nearly all 110 seconds is framework overhead.

Pandas incurs none of it: one process, data resident in RAM, no serialisation, no
shuffle, no disk spill.

### 13.4 When distributed processing becomes necessary

The Pandas run loaded **one column** to remain within memory. Loading all ten
columns across 9.5 million rows would have required several gigabytes. This is
the real constraint, and it defines the crossover:

**Pandas fails when the working set exceeds available RAM.** On this machine with
8 GB available, that limit is roughly 30–50 million records with full columns. At
100 million records Pandas cannot run at all, while MapReduce executes the same
code with proportionally more map tasks.

Distributed processing becomes the correct choice when:

1. **The working set exceeds single-machine memory.** The decisive factor.
2. **Data is already distributed** across cluster nodes, making local processing move terabytes over the network.
3. **Fault tolerance is required.** A multi-hour job on one machine loses everything on failure; MapReduce re-executes only the failed task.
4. **A hard deadline requires horizontal scaling** — adding machines rather than waiting.

**None of these conditions applies to this dataset.** The honest conclusion is
that MapReduce is the wrong tool for 9.5 million records on a machine with
sufficient RAM, and this experiment demonstrates the cost of using it anyway.

The value of the exercise is not that Hadoop won — it did not — but that the
programming model, the shuffle mechanics, and the operational behaviour are now
understood, and they transfer directly to Spark and every other distributed
engine.

**[SCREENSHOT 10 — pandas_comparison.py output alongside the MapReduce counters]**
![alt text](10_performance_comparison.png)

---

## 14. Business Insights

### 14.1 Answers to the business questions

**(a) Busiest hour for taxi demand?**
18:00, with 690,932 trips. The quietest is 04:00 with 58,117 — a 12:1 ratio. The
evening peak substantially exceeds the morning peak.

**(b) Busiest day of the week?**
Thursday, with 1,526,638 trips. Weekend per-day demand (1,339,912) is
statistically indistinguishable from weekday demand (1,351,808).

**(c) Which pickup zones generate the most trips?**
Zone 161 (447,805), 237 (434,219), 132 (416,447), 236 (412,395).

**(d) Which pickup zones generate the most revenue?**
Zone 132/JFK ($33.9M), 138/LaGuardia ($18.8M), 161 ($10.9M). **The volume and
revenue rankings differ**: zone 161 leads on trips but ranks third on revenue.

**(e) Which payment method contributes the most revenue?**
Credit card: $207.4M, 86.0% of total revenue across 85.9% of trips.

**(f) Do credit-card users generate more tips?**
Recorded tips: yes, dramatically — $4.21 average versus effectively $0 for cash.
But this is a **measurement artefact**, not a behavioural finding. Cash tips are
handed to the driver and never recorded by the meter. Average fares are nearly
identical across the two methods, indicating comparable trip populations.

**(g) Which distance category produces the highest average fare?**
20+ miles, at $89.87. However, fare **per mile** is highest for 0–2 mile trips
($8.85 vs $3.73), making short trips 2.4× more profitable per mile travelled.

**(h) Most frequently travelled routes?**
237→236 (63,572 trips), 236→237 (56,343), 236→236 (45,017). All are short
intra-Midtown movements.

**(i) Are the most frequent routes also the most profitable?**
**No.** Only one route appears in both top-20 lists. The highest-revenue route
(132→265, $125.79/trip) carries a quarter of the trips of the highest-frequency
route (237→236, $15.60/trip).

**(j) What percentage of records contain potential anomalies?**
11.45% — 1,093,638 of 9,554,778 records fail at least one validity check. The
largest single category is missing passenger count at 7.87%.

**(k) What transportation insights can management derive?**
Detailed below.

**(l) When does Hadoop MapReduce provide a meaningful advantage?**
Detailed in Section 13.4. Not at this data volume.

### 14.2 Management recommendations

**1. Fleet positioning must choose an objective.** Volume and revenue optimise to
different locations. A driver maximising trips per shift belongs in Midtown
(zones 161, 236, 237); a driver maximising revenue per shift belongs in the JFK
queue. These are mutually exclusive strategies, and the data quantifies the
trade-off precisely: $81 per trip at JFK versus $24 in Midtown, against 3.3×
higher trip frequency.

**2. Demand is evening-driven, not commuter-driven.** The 18:00 peak exceeds the
09:00 peak by 70%, and weekend demand matches weekday demand. Staffing models
built on a commuter assumption are mis-specified. The 16:00–20:00 window carries
27% of daily volume.

**3. Short trips are the profitability engine.** At $8.85 per mile, sub-2-mile
trips more than double the per-mile yield of long-haul work. 54% of all trips
fall in this category. Utilisation strategy should favour rapid turnover of short
trips over waiting for airport fares — except where the airport queue is short.

**4. Tip data is structurally incomplete.** Roughly 14% of trips have unmeasurable
tip revenue. Any driver-compensation model, service-quality metric, or incentive
scheme built on recorded tips is blind to cash customers. This should be stated
as a limitation in any downstream analysis.

**5. 21,507 trips warrant fraud investigation.** Fares exceeding $100 per mile are
not explicable by normal pricing. At 0.23% of trips this is a tractable
investigation list rather than a systemic problem, and it is the most directly
actionable output of the analysis.

**6. Route concentration enables targeted operations.** Only 37% of possible
zone-pairs ever occur, and the top 20 routes account for a disproportionate share
of volume. Dispatch optimisation, driver guidance and demand prediction can focus
on a manageable corridor set rather than the full 68,644-cell matrix.

---

## 15. Limitations

**Single-node cluster.** All daemons ran on one machine with replication factor 1.
This means no data redundancy (reflected in the 4 under-replicated blocks), no
network shuffle between physical hosts, and no fault tolerance. Performance
characteristics differ substantially from a real multi-node cluster, where
network transfer during shuffle typically dominates.

**Windows platform.** As documented in Section 4.2, four of five environment
failures were Windows-specific and would not occur on Linux. The
domain-authentication dependency in particular makes this configuration unsuitable
for production.

**Three months of data.** January–March 2024 captures winter and early spring
only. Seasonal effects — summer tourism, holiday periods, weather — are not
represented. Conclusions about demand patterns should not be extrapolated across
a full year.

**Yellow taxi only.** Green taxi, for-hire vehicle and high-volume FHV (Uber,
Lyft) records were excluded. Yellow taxis are a declining share of NYC
for-hire trips, so this analysis describes one segment of a larger market.

**Zone IDs not resolved to names.** Results reference numeric `LocationID` values.
Joining the TLC zone lookup table would improve readability, though it does not
change any finding.

**Cash tips unmeasurable.** As discussed, roughly 14% of trips have no recorded
tip data.

**Negative amounts excluded rather than reconciled.** Refunds and chargebacks were
excluded from revenue sums rather than netted, marginally overstating reported
revenue.

**Trip duration analysis not completed.** Section 8(h) of the assignment was not
implemented due to time constraints.

---

## 16. Conclusion

This project implemented a complete Hadoop analytics pipeline over 9.55 million
NYC taxi trip records: HDFS storage across a nine-directory structure, eight
Python mapper/reducer pairs executed through Hadoop Streaming, a two-stage
MapReduce workflow with persisted intermediate output, YARN-managed execution
with full application evidence, and a like-for-like performance benchmark against
Pandas.

Three findings stand out.

**Analytically**, trip volume and revenue are inversely related in this network.
The highest-traffic zones and routes are not the highest-earning ones, and the
gap is large — 3.3× on revenue per trip at zone level, and 8× at route level.
This single fact reframes fleet strategy from "go where the demand is" to "decide
what you are optimising for."

**Technically**, the map-shuffle-reduce model imposes specific design discipline:
keys must be constructed to exploit sort order, each input split must be
independently interpretable, reducers must hold constant state, and mappers must
tolerate malformed input. These constraints are not incidental — they are what
makes horizontal scaling possible, and they recur in every distributed engine
built since.

**Practically**, MapReduce was 6.5× slower than Pandas on this dataset while
producing identical results. This is the correct outcome at 9.5 million records
and the correct conclusion to draw: distributed processing is a response to data
that does not fit, not a universally superior technique. The judgement of when to
reach for it is more valuable than the ability to operate it.

---

## 17. References

New York City Taxi & Limousine Commission. *TLC Trip Record Data*.
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Apache Software Foundation. *Apache Hadoop 3.3.6 Documentation*.
https://hadoop.apache.org/docs/r3.3.6/

Apache Software Foundation. *Hadoop Streaming*.
https://hadoop.apache.org/docs/r3.3.6/hadoop-streaming/HadoopStreaming.html

Apache Software Foundation. *Apache Parquet Documentation*.
https://parquet.apache.org/docs/

Dean, J. and Ghemawat, S. (2004). *MapReduce: Simplified Data Processing on Large
Clusters*. OSDI'04.

White, T. (2015). *Hadoop: The Definitive Guide*, 4th edition. O'Reilly Media.

---

## Appendix A — Repository contents

```
mappers/           Eight Python mapper programs
reducers/          Eight Python reducer programs
results/           Job output retrieved from HDFS
charts/            Seven generated visualisations
screenshots/       Evidence captures
commands.txt       All HDFS and Hadoop Streaming commands
convert_to_csv.py  Parquet to CSV conversion
pandas_comparison.py  Single-machine benchmark
make_charts.py     Visualisation generation
README.md          Environment and execution instructions
REPORT.md          This document
```

## Appendix B — Reproduction

Full commands are in `commands.txt`. Summary sequence:

```
start-dfs.cmd
start-yarn.cmd
jps
yarn node -list -all

python convert_to_csv.py

hdfs dfs -mkdir -p /taxi_project/input/raw
hdfs dfs -put yellow_tripdata_2024-0*.csv /taxi_project/input/raw/

hadoop jar %HADOOP_HOME%\share\hadoop\tools\lib\hadoop-streaming-3.3.6.jar ^
  -files "mappers/mapper_hourly.py,reducers/reducer_hourly.py" ^
  -input /taxi_project/input/raw ^
  -output /taxi_project/output/hourly ^
  -mapper "python mapper_hourly.py" ^
  -reducer "python reducer_hourly.py"
```
