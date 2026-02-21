I created this collection of PySpark scripts to showcase how I approach common data engineering problems — covering reading from various file formats, handling data quality issues, basic transformations, different types of joins, and writing data efficiently. The data files included are intentionally messy to reflect what I actually deal with on the job.
## Repository Structure
```
PySpark/
    ├── 01_Read_Sources/
    │   ├── csv_files/
    │   │   ├── sales.csv
    │   │   ├── sales_corrupted.csv
    │   │   ├── sales_no_header.csv
    │   │   ├── sales_pipe.csv
    │   │   ├── sales_pipe_single_quote.csv
    │   │   ├── sales_pipe_single_quote_escape.csv
    │   │   └── sales_pipe_single_quote_escape_multiline.csv
    │   ├── json_files/
    │   │   ├── sales_corrupted.json
    │   │   ├── sales_multi_line.json
    │   │   └── sales_single_line.json
    │   ├── log_files/
    │   │   ├── log1.txt
    │   │   └── log2.txt
    │   ├── Spark_Corrupted_Records.py
    │   ├── Spark_Read_CSV.py
    │   ├── Spark_Read_Json.py
    │   ├── Spark_Read_Write_Parquet.py
    │   └── Text_Files.py
    │
    ├── 02_Transformations/
    │   ├── 1_Basic.py
    │   ├── 2_Types_of_Data.py
    │   ├── 3_Aggregations.py
    │   └── 4_Complex_Types.py
    │
    ├── 03_Join/
    │   ├── Join_Strategies.py
    │   └── Join_Types.py
    │
    └── 04_Write_Data/
        ├── Merge_Into.py
        └── Write_Options.py
```
## Module Breakdown
### 01 · Reading Sources
This section covers ingesting data from different formats and handling edge cases that come up in real projects:
| Script | Description |
|---|---|
| `Spark_Read_CSV.py` | Reading CSVs with various delimiters, quote characters, escape characters, multi-line records, and missing headers |
| `Spark_Read_Json.py` | Parsing single-line and multi-line JSON, handling schema inference |
| `Spark_Corrupted_Records.py` | Dealing with malformed data using `PERMISSIVE`, `DROPMALFORMED`, and `FAILFAST` modes |
| `Spark_Read_Write_Parquet.py` | Reading and writing Parquet files with schema evolution |
| `Text_Files.py` | Processing raw log files and unstructured text |
### 02 · Transformations
The core Spark transformations I use day-to-day:
| Script | Description |
|---|---|
| `1_Basic.py` | `select`, `filter`, `withColumn`, `drop`, `alias`, `cast` |
| `2_Types_of_Data.py` | Working with StringType, DateType, TimestampType, ArrayType, MapType, StructType |
| `3_Aggregations.py` | `groupBy`, `agg`, `pivot`, window functions |
| `4_Complex_Types.py` | Explode, flatten, `array_contains`, `map_keys`, struct operations |
### 03 · Joins
| Script | Description |
|---|---|
| `Join_Types.py` | Inner, Left, Right, Full Outer, Semi, Anti, Cross joins |
| `Join_Strategies.py` | Broadcast joins, Sort-Merge joins, shuffle optimization, skew handling |
### 04 · Writing Data
| Script | Description |
|---|---|
| `Write_Options.py` | Save modes (`overwrite`, `append`, `ignore`, `error`), partitioning, bucketing |
