\# spark-data-engineering-portfolio



> A comprehensive, hands-on collection of \*\*Apache PySpark\*\* and \*\*Databricks\*\* projects covering real-world data engineering patterns — from ingestion and transformation to Delta Lake and production-grade optimizations.



---



\## About This Repository



This portfolio demonstrates practical expertise in distributed data processing using \*\*PySpark\*\*, \*\*Delta Lake\*\*, and \*\*Databricks\*\*. Each module is structured to reflect real-world engineering challenges: reading messy data sources, applying complex transformations, writing optimized outputs, and managing data quality at scale.



---



\## Repository Structure



```

spark-data-engineering-portfolio/

│

├── 01\_PySpark/

│   ├── 01\_Read\_Sources/

│   │   ├── csv\_files/

│   │   │   ├── sales.csv

│   │   │   ├── sales\_corrupted.csv

│   │   │   ├── sales\_no\_header.csv

│   │   │   ├── sales\_pipe.csv

│   │   │   ├── sales\_pipe\_single\_quote.csv

│   │   │   ├── sales\_pipe\_single\_quote\_escape.csv

│   │   │   └── sales\_pipe\_single\_quote\_escape\_multiline.csv

│   │   ├── json\_files/

│   │   │   ├── sales\_corrupted.json

│   │   │   ├── sales\_multi\_line.json

│   │   │   └── sales\_single\_line.json

│   │   ├── log\_files/

│   │   │   ├── log1.txt

│   │   │   └── log2.txt

│   │   ├── JDBC.py

│   │   ├── Spark\_Corrupted\_Records.py

│   │   ├── Spark\_Read\_CSV.py

│   │   ├── Spark\_Read\_Json.py

│   │   ├── Spark\_Read\_Write\_Parquet.py

│   │   └── Text\_Files.py

│   │

│   ├── 02\_Transformations/

│   │   ├── 1\_Basic.py

│   │   ├── 2\_Types\_of\_Data.py

│   │   ├── 3\_Aggregations.py

│   │   └── 4\_Complex\_Types.py

│   │

│   ├── 03\_Join/

│   │   ├── Join\_Strategies.py

│   │   └── Join\_Types.py

│   │

│   ├── 04\_UI\_Demo/

│   │   └── UI\_Demo.py

│   │

│   ├── 05\_Write\_Data/

│   │   ├── Merge\_Into.py

│   │   └── Write\_Options.py

│   │

│   └── 06\_UDF/

│       └── UDF.py

│

├── 02\_Delta\_Lake/

│   ├── 01\_Delta\_Lake\_Overview.py

│   ├── 02\_Optimize\_and\_Vacuum.py

│   ├── 03\_Clone\_Shallow\_vs\_Deep.py

│   ├── 04\_Liquid\_Clustering.py

│   └── 05\_Change\_Data\_Feed.py

│

└── 03\_Databricks\_Adhoc/

&nbsp;   ├── DQX\_DEMO.py

&nbsp;   └── Debug\_Mode.py

```



---



\## Module Breakdown



\### 01 · PySpark Core



\#### Reading Sources

Covers ingesting data from a wide range of formats and handling edge cases gracefully:



| Script | Description |

|---|---|

| `Spark\_Read\_CSV.py` | Reading CSVs with various delimiters, quote characters, escape characters, multi-line records, and missing headers |

| `Spark\_Read\_Json.py` | Parsing single-line and multi-line JSON, handling schema inference |

| `Spark\_Corrupted\_Records.py` | Strategies for dealing with malformed/corrupt records using `PERMISSIVE`, `DROPMALFORMED`, and `FAILFAST` modes |

| `Spark\_Read\_Write\_Parquet.py` | Reading and writing Parquet files with schema evolution |

| `JDBC.py` | Connecting to relational databases via JDBC |

| `Text\_Files.py` | Processing raw log files and unstructured text |



\#### Transformations

Core Spark transformations applied to real data:



| Script | Description |

|---|---|

| `1\_Basic.py` | `select`, `filter`, `withColumn`, `drop`, `alias`, `cast` |

| `2\_Types\_of\_Data.py` | Working with StringType, DateType, TimestampType, ArrayType, MapType, StructType |

| `3\_Aggregations.py` | `groupBy`, `agg`, `pivot`, window functions |

| `4\_Complex\_Types.py` | Explode, flatten, `array\_contains`, `map\_keys`, `struct` operations |



\#### Joins

| Script | Description |

|---|---|

| `Join\_Types.py` | Inner, Left, Right, Full Outer, Semi, Anti, Cross joins |

| `Join\_Strategies.py` | Broadcast joins, Sort-Merge joins, shuffle optimization, skew handling |



\#### Writing Data

| Script | Description |

|---|---|

| `Write\_Options.py` | Save modes (`overwrite`, `append`, `ignore`, `error`), partitioning, bucketing |

| `Merge\_Into.py` | Upsert patterns using Delta Lake `MERGE INTO` |



\#### User Defined Functions (UDFs)

| Script | Description |

|---|---|

| `UDF.py` | Python UDFs, Pandas UDFs (vectorized), and when to avoid UDFs |



\#### Spark UI

| Script | Description |

|---|---|

| `UI\_Demo.py` | Navigating the Spark UI — stages, tasks, DAGs, and diagnosing performance bottlenecks |



---



\### 02 · Delta Lake



Delta Lake brings ACID transactions, time travel, and schema enforcement to data lakes.



| Script | Description |

|---|---|

| `01\_Delta\_Lake\_Overview.py` | Creating, reading, and updating Delta tables; time travel with `versionAsOf` / `timestampAsOf` |

| `02\_Optimize\_and\_Vacuum.py` | Compacting small files with `OPTIMIZE`, reclaiming storage with `VACUUM` |

| `03\_Clone\_Shallow\_vs\_Deep.py` | When to use shallow vs. deep clones; use cases for testing and backups |

| `04\_Liquid\_Clustering.py` | Next-generation alternative to partitioning for flexible and efficient data layout |

| `05\_Change\_Data\_Feed.py` | Tracking row-level changes (insert/update/delete) for CDC pipelines |



---



\### 03 · Databricks Ad Hoc



| Script | Description |

|---|---|

| `DQX\_DEMO.py` | Data quality checks using Databricks Labs DQX — profiling, expectation validation |

| `Debug\_Mode.py` | Debugging Databricks notebooks: cluster logs, display, `%sql`, widget interactions |



---



\## Tech Stack



| Tool | Version |

|---|---|

| Apache Spark | 3.4+ |

| PySpark | 3.4+ |

| Delta Lake | 3.x |

| Databricks Runtime | 13.x / 14.x LTS |

| Python | 3.10+ |



---



\## Getting Started



\### Run Locally (PySpark)



```bash

\# Clone the repo

git clone https://github.com/<your-username>/spark-data-engineering-portfolio.git

cd spark-data-engineering-portfolio



\# Install dependencies

pip install pyspark delta-spark



\# Run a script

python 01\_PySpark/01\_Read\_Sources/Spark\_Read\_CSV.py

```



\### Run on Databricks



1\. Import the `.py` files as notebooks via \*\*Workspace → Import\*\*

2\. Attach to a cluster with Databricks Runtime 13.x+ (Delta Lake pre-installed)

3\. Run cells interactively or schedule as a Job



---



\## Key Concepts Demonstrated



\- Schema inference vs. explicit schema definition

\- Handling corrupt and malformed data at scale

\- Broadcast vs. Sort-Merge join selection

\- Window functions for ranking, lag/lead, running totals

\- Delta Lake ACID transactions and time travel

\- CDC (Change Data Capture) with Change Data Feed

\- UDF vs. Pandas UDF performance trade-offs

\- Liquid Clustering for dynamic data skipping

\- Shallow vs. deep clone strategies

\- Data quality enforcement with DQX





---



\## License



This project is licensed under the \[MIT License](LICENSE).

