# PySpark:

# 1. Introduction to PySpark:
# 1.1 PySpark Overview:
PySpark is the Python API for Apache Spark, used to process large datasets in a distributed computing environment.It allows you to write Spark applications using Python instead of Scala/Java.
# It is commonly used for:
* Big data processing
  
* Distributed computing
  
* ETL pipelines
  
* Data analytics
# It supports:
* Spark SQL
  
* Spark MLib
  
* Dataframe
  
* RDD

 #  Key Features of PySpark
  | Feature                    | Explanation                                  |
| -------------------------- | -------------------------------------------- |
| **Distributed Processing** | Processes huge datasets across many machines |
| **In-memory computation**  | Faster than traditional Hadoop MapReduce     |
| **Fault tolerance**        | Data is replicated → no data loss            |
| **Scalable**               | Works on 1 machine or 1000 machines          |
| **Supports multiple APIs** | RDD, DataFrame, SQL, MLlib, Streaming        |

# 1.2 Introduction to PySpark and its role in big data processing:
PySpark is the Python API for Apache Spark that allows you to perform big data processing using Python.In big data, datasets are too large to process on a single machine.

# PySpark helps by:
* Distributing data across multiple machines (nodes)
  
* Processing data in parallel
  
* Using in-memory computation (very fast)

* Providing fault tolerance
# PySpark allows:

* Processing terabytes/petabytes using clusters

* Real-time + batch processing
# Role of PySpark in Big Data

* Handles large datasets efficiently

* Processes data in distributed clusters

* Supports ETL pipelines (Extract-Transform-Load)

* Used in Data Engineering + Machine Learning pipelines

* High performance for analytics & reporting

* Running jobs in parallel → faster execution

For example, PySpark reads TBs of logs, identifies which products users viewed, and then recommends similar products. Since the data is too large for Pandas, PySpark distributes the processing across multiple nodes, making it very fast.”

# 1.3 Python API for Apache Spark

PySpark provides Python functions to interact with Spark.Spark is originally written in Scala.But PySpark provides Python bindings (APIs) to use Spark features such as:

RDD

DataFrame

SQL

MLlib

Streaming
# 2. Revision on Spark Architecture: 
2.1 Revising the architecture of Spark.
Spark follows Master–Slave architecture.
# Simple Flow:
Your PySpark program → runs on Driver

Driver splits job into tasks

Cluster manager assigns tasks to Executors

Executors process data in parallel

Results are sent back to Driver

# 2.2 Integration with Spark components like Driver and Executors.
# Driver Program
Runs your Python code

Creates SparkSession

Plans tasks using DAG Scheduler

Sends tasks to executors
# Executors
Run tasks given by driver

Perform transformations like map, filter

Store data in memory (RDD/DataFrame caching)

Return results to Driver
# 3. Revision on Spark Components:
SparkSession:Entry point for DataFrame, SQL, RDD, ML and it Combines SQLContext, HiveContext, and SparkContext

RDD:Low-level distributed dataset.It supports transformations (map, filter) and it has no schema.

DataFrame:High-level structured dataset and it Has schema (rows & columns).Optimized by Catalyst optimizer

DAG Scheduler:Converts execution plan into stages, Handles failures

# 4. SparkSession:
SparkSession is the unified entry point to all Spark functionalities like DataFrame, SQL, and streaming.
# 4.1 Explanation of SparkSession as the entry point to PySpark. 
SparkSession is the unified entry point to use Spark in Python (DataFrame, SQL, streaming, catalog, configuration).You cannot use DataFrames or SQL without SparkSession.
It contains a SparkContext inside (spark.sparkContext) so you get both high-level (DataFrame/SQL) and low-level (RDD) APIs from the same object.
# It provides access to:
* SparkContext: SparkContext is the entry point to the Spark Core engine.
 # create SparkContext
 from pyspark import SparkContext
 
sc = SparkContext(master="local[*]", appName="OldSQLApp")

* SQLContext: old interface for DataFrame & SQL operations.
 # create SQLContext
  from pyspark.sql import SQLContext
  
sqlContext = SQLContext(sc)
# 4.2 Configuring and creating a SparkSession.
<img width="898" height="111" alt="image" src="https://github.com/user-attachments/assets/1ca43d94-9c72-4498-b33d-f41e24e93866" />

# 5. DataFrame API:
A PySpark DataFrame is a distributed table consisting of rows and columns, similar to SQL or Pandas DataFrame — but optimized for big data and parallel processing.Immutable
# 5.1 Overview of the DataFrame API in pyspark:
# Key Features:
Schema-based (column names + data types)

Distributed (works across multiple nodes)

Optimized using Catalyst Optimizer

Lazy evaluation

Supports SQL-like operations (select, filter, groupBy)

Can handle terabytes of data
# 5.2 Comparison: PySpark DataFrame vs Pandas DataFrame
| Feature         | **PySpark DataFrame**                     | **Pandas DataFrame**          |
| --------------- | ----------------------------------------- | ----------------------------- |
| Data Size       | TB/PB (big data)                          | MB/GB (small data)            |
| Storage         | Distributed across cluster                | Stored in single machine RAM  |
| Language        | Runs on Spark engine (Java/Scala backend) | Pure Python                   |
| Execution       | Lazy                                      | Immediate                     |
| Optimization    | Catalyst + Tungsten                       | No optimizer                  |
| Speed           | Very fast for large data                  | Fast for small data           |
| Fault Tolerance | Yes (RDD lineage)                         | No                            |
| Parallelism     | Cluster-wide                              | Single machine                |
| Use Case        | Big Data, ETL, ML pipelines               | Small datasets, data analysis |

# 6. Transformations and Actions: 
A transformation is an operation that creates a new RDD/DataFrame from an existing one.
# Examples of transformations

map() 

filter()

flatMap()

groupBy()

select()

where()

join()

orderBy()

repartition()

coalesce()

An action triggers the execution of the DAG and returns a result.

Examples of actions

collect()

show()

count()

first()

take()

saveAsTextFile()

write() (for DataFrame)

reduce()

# 6.2 Examples of Common Transformations and Actions:
# Map:
Applies a function to each element
<img width="840" height="637" alt="image" src="https://github.com/user-attachments/assets/2a413339-7039-4d81-972c-035fe0061d5b" />

# Filter:
Filter the element based on the given condition
<img width="743" height="644" alt="image" src="https://github.com/user-attachments/assets/65cef3fa-387e-4431-a6b7-de44092abb41" />

# Joins:
A JOIN combines two DataFrames based on a common/key column.
<img width="743" height="645" alt="image" src="https://github.com/user-attachments/assets/fbaae850-76e6-4684-987c-9d03d9e13b42" />

# Order by:
It will return the values as ascending or descending order.
<img width="826" height="607" alt="image" src="https://github.com/user-attachments/assets/95e82328-6a95-406c-aebc-5fb9f254b50c" />

# Group by:
It will group the results.
<img width="1022" height="601" alt="image" src="https://github.com/user-attachments/assets/9cfc3db0-daf7-4d10-a523-d534a57a4d57" />

# 7 Revision on Spark RDDs:
  # 7.1 Overview of RDDs in PySpark
RDD (Resilient Distributed Dataset) is the fundamental data structure in Spark.It is a distributed collection of data, processed in parallel across multiple nodes in a cluster.
# 7.2 Differences between RDDs and DataFrames:
| **Feature**         | **RDD**                                                    | **DataFrame**                                       |
| ------------------- | ---------------------------------------------------------- | --------------------------------------------------- |
| **Definition**      | Low-level distributed data structure                       | High-level tabular data structure                   |
| **Schema**          | No schema (unstructured data)                              | Has schema (columns + datatypes)                    |
| **Ease of Use**     | Complex (requires custom code using map, filter, reduce)   | Very easy (SQL-like operations, built-in functions) |
| **Optimization**    | No automatic optimization                                  | Optimized by Catalyst Optimizer                     |
| **Speed**           | Slower                                                     | Much faster                                         |
| **Memory Usage**    | High                                                       | Low (uses Tungsten engine for optimized memory)     |
| **APIs Supported**  | Only functional APIs (map, flatMap, reduce)                | SQL API, DataFrame API, MLlib                       |
| **Error Handling**  | Harder (no schema validation)                              | Easier (schema checks errors early)                 |
| **Use Cases**       | Unstructured data, low-level transformations, custom logic | Structured data, analytics, ETL pipelines           |
| **Lazy Evaluation** | Yes                                                        | Yes                                                 |

# 8 Data Structures in PySpark:
PySpark mainly works with the following distributed data structures:
RDD (Resilient Distributed Dataset):Low-level distributed list

DataFrame:Distributed table with schema

Row:Single record in DataFrame

Column:Expression representing a column

<img width="756" height="604" alt="image" src="https://github.com/user-attachments/assets/86faf59d-8a1a-46b3-8e19-e1d4a043722c" />

<img width="644" height="643" alt="image" src="https://github.com/user-attachments/assets/2654956e-80f0-49e6-ac18-02b2f50388cf" />


# 9 SparkContext:
SparkContext is the core entry point for low-level Spark functionality.It allows you to work with RDDs, manage cluster resources, and communicate with the Spark cluster.Today, most applications use SparkSession, but SparkContext still exists inside SparkSession and is used for RDD operations.

# 9.1 The Role of SparkContext in PySpark Applications:
It is the main entry point to Spark's lower-level API (RDD API). It connects your PySpark application to the Spark cluster. It is responsible for:
Creating RDDs

Managing workers/executors

Distributing tasks

Coordinating jobs and stages

# 10. PySpark DataFrames: 
A PySpark DataFrame is a distributed collection of data organized in rows and columns, similar to a table in a database or an Excel sheet.
It is built on top of the Spark SQL engine, meaning:

It supports SQL queries

It automatically handles optimizations using Catalyst Optimizer

It is faster than RDDs due to optimized execution

# 10.1 Introduction to PySpark DataFrames
DataFrame is a distributed collection of data organized into named columns.
Supports SQL queries, transformations, and actions.

# Operations on DataFrames
Filtering: df.filter(df['age'] > 25)

Selecting: df.select('name', 'salary')

Aggregating: df.groupBy('department').agg({'salary':'avg'})

Adding Columns: df.withColumn('new_col', df['salary']*2)

# PySpark SQL
# Integration of SQL Queries with PySpark

PySpark allows SQL queries on DataFrames.

Provides flexibility to use familiar SQL syntax.

# Registering DataFrames as Temporary SQL Tables
# Register DataFrame as temp table
df.createOrReplaceTempView("employees")
# Execute SQL query

result = spark.sql("SELECT name, salary FROM employees WHERE age > 25")

result.show()

Temporary views are session-scoped.

Useful for complex SQL queries on DataFrames.

# Persist and Cache in PySpark
# Overview
In PySpark, transformations are lazy — meaning data isn’t computed until an action (like count() or show()) is called.
If you reuse the same RDD or DataFrame multiple times, Spark will recompute it each time, which can be expensive.

To avoid recomputation and improve performance, you can cache or persist the dataset in memory (and optionally on disk).

# cache() — Store in Memory Only
# Definition:
cache() stores the RDD or DataFrame in memory only (MEMORY_ONLY storage level).

# Syntax:
df.cache()
# Example:
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("CacheExample").getOrCreate()

data = [
    (1, "Alice", 4000),
    
    (2, "Bob", 5000),
    
    (3, "Charlie", 4500),
    
    (4, "David", 7000)
]
columns = ["ID", "Name", "Salary"]

df = spark.createDataFrame(data, columns)

# Cache the DataFrame
df.cache()

# Trigger an action to store data in memory
df.show()

# Reuse it (no recomputation now)
high_salary = df.filter(col("Salary") > 4500)
high_salary.show()
# Notes:
The first action (show()) will compute and cache the DataFrame.
The next actions will read it directly from memory, improving performance.
# persist() — Store with a Custom Storage Level
# Definition:
persist() allows you to choose where and how to store your dataset.

# Syntax:
df.persist(storageLevel)
# Storage Levels:
| **Storage Level**       | **Description**                                                                                                                             |
| ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| **MEMORY_ONLY**         | Default for `cache()` → Stores **deserialized** objects in **memory only**. If memory is not enough, missing partitions are **recomputed**. |
| **MEMORY_AND_DISK**     | Tries to store in memory; if not enough memory, spills the remaining partitions to **disk**. No recomputation needed.                       |
| **DISK_ONLY**           | Stores **all data on disk only**. No memory used. Slowest, but safest for huge data.                                                        |
| **MEMORY_ONLY_SER**     | Stores **serialized (compressed)** objects in memory → uses **less RAM** but needs CPU for serialization/deserialization.                   |
| **MEMORY_AND_DISK_SER** | Stores data **serialized in memory**; if memory insufficient, spills **serialized** data to disk.                                           |
| **OFF_HEAP**            | Stores data in **off-heap memory (outside JVM heap)**. Requires extra configuration. Good for Tungsten engine.                              |

# Example:

from pyspark import StorageLevel

# Persist DataFrame with MEMORY_AND_DISK

df.persist(StorageLevel.MEMORY_AND_DISK)

# Trigger caching

df.count()

# Reuse without recomputation
df.filter(col("Salary") > 4500).show()

# Difference Between cache() and persist()
| **Feature**               | **cache()**                           | **persist()**                                                              |
| ------------------------- | ------------------------------------- | -------------------------------------------------------------------------- |
| **Default Storage Level** | **MEMORY_ONLY**                       | **Customizable** (e.g., MEMORY_AND_DISK, DISK_ONLY, MEMORY_ONLY_SER, etc.) |
| **Flexibility**           | Less flexible                         | More flexible                                                              |
| **When to Use**           | When data fits comfortably in memory  | When data is large or memory is limited                                    |
| **Performance**           | Faster (keeps deserialized objects)   | May be slower depending on storage level (serialization may happen)        |
| **Control Over Storage**  | No control (always MEMORY_ONLY)       | Full control (choose level using StorageLevel)                             |
| **If Memory Is Full**     | Missing partitions are **recomputed** | Missing partitions are **spilled to disk** (if MEMORY_AND_DISK)            |
| **Code Example**          | `df.cache()`                          | `df.persist(StorageLevel.MEMORY_AND_DISK)`                                 |


# Removing Cached/Persisted Data

You can uncache or remove persisted data to free up memory:
df.unpersist()

# When to Use
Use cache() when:
*Dataset fits easily in memory.

*It is reused multiple times in your job.
# Use persist() when:
*Dataset is large or partially reused.

*You need a custom storage level (e.g., MEMORY_AND_DISK).
# Performance Tip
Always perform a lazy action (like count(), show(), or collect()) after calling cache() or persist() to materialize the dataset — otherwise, it won’t actually be stored yet.

df.cache()
df.count()   # triggers computation and stores data

# Example Summary Code
from pyspark.sql import SparkSession

from pyspark import StorageLevel

from pyspark.sql.functions import col

spark = SparkSession.builder.appName("CachePersistExample").getOrCreate()

data = [

    (101, "Alice", "IT", 50000),
    
    (102, "Bob", "HR", 40000),
    
    (103, "Charlie", "Finance", 55000),
    
    (104, "David", "IT", 60000)
]
columns = ["ID", "Name", "Dept", "Salary"]

df = spark.createDataFrame(data, columns)

# Cache example
df.cache()

df.count()

# Persist example
df.persist(StorageLevel.MEMORY_AND_DISK)

df.show()

# Unpersist

df.unpersist()


