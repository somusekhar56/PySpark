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
<img width="844" height="601" alt="image" src="https://github.com/user-attachments/assets/6a6ff065-016b-417c-b873-38da7ac888ef" />

Filtering: df.filter(df['age'] > 20)
<img width="974" height="596" alt="image" src="https://github.com/user-attachments/assets/3a7cf8e0-d1a4-4e19-90d9-ce94a18475e4" />

df.select(avg(df.age)).show()
<img width="972" height="633" alt="image" src="https://github.com/user-attachments/assets/bff41fa1-5286-4b68-97eb-9a45ad8448ea" />
# Adding coloumn
df = df.withColumn("doubleage",df.age * 2).show()
<img width="696" height="576" alt="image" src="https://github.com/user-attachments/assets/4fe02d68-45ef-40f2-89aa-9d4dd03e5631" />

df.orderBy(col("age").desc()).show()
<img width="663" height="628" alt="image" src="https://github.com/user-attachments/assets/8905da90-6607-41b7-be97-60cc0cfb0b69" />

df.groupBy("gender").count().show()
<img width="1057" height="532" alt="image" src="https://github.com/user-attachments/assets/257c2f28-bb38-43d6-af0e-db47f7fe3e8f" />

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

# 13. General DataFrame Functions: 


| Function         | Purpose                     |
| ---------------- | --------------------------- |
| show()           | Display the DataFrame       |
| collect()        | Bring all rows to driver    |
| take(n)          | Get first n rows            |
| printSchema()    | Show structure of DataFrame |
| count()          | Number of rows              |
| select()         | Choose specific columns     |
| filter()/where() | Filter rows                 |
| like()           | Pattern matching            |
| sort()           | Sorting rows                |
| describe()       | Summary statistics          |
| columns          | List of column names        |

# show()
df.show()
Displays the entire DataFrame.

<img width="628" height="599" alt="image" src="https://github.com/user-attachments/assets/52600788-84be-45d0-94de-5e3779bef099" />

# collect()
df.collect()

Returns all rows as a list of Row objects.

<img width="1033" height="633" alt="image" src="https://github.com/user-attachments/assets/a8dc10a0-893f-4b6a-aec0-df005e48602e" />

# take(n):
df.take(2)

Takes the first n rows.

<img width="754" height="642" alt="image" src="https://github.com/user-attachments/assets/3ff03705-3c99-4032-b782-0531e50e8a85" />

# printSchema()
df.printSchema()

Displays column structure.

<img width="1091" height="627" alt="image" src="https://github.com/user-attachments/assets/74c1c4cf-b522-439a-a13a-f51def6d5552" />

# count():

df.count()

Counts total rows.

<img width="712" height="636" alt="image" src="https://github.com/user-attachments/assets/615b1080-dc3d-44c7-a98e-21be904709cd" />

# select()
Selects specific columns.

df.select("id", "name").show()

<img width="735" height="605" alt="image" src="https://github.com/user-attachments/assets/907a7f68-178e-46a4-8a02-8b11be7cd67d" />

# filter() / where():
Filters based on conditions.

df.filter(col("age") > 30).show()

<img width="904" height="608" alt="image" src="https://github.com/user-attachments/assets/fb932ada-b323-43c5-9962-9583ad60bf86" />

df.where(col("dept") == "IT").show()

<img width="993" height="605" alt="image" src="https://github.com/user-attachments/assets/7233000a-2188-478e-bdd3-40f7b3aec303" />

# like():

# Names starting with 'A'

df.filter(df.name.like("A%")).show()

# Names ending with 'e'

df.filter(df.name.like("%e")).show()

# Names containing 'a'

df.filter(df.name.like("%a%")).show()

# Department starting with 'F'

df.filter(df.dept.like("F%")).show()

like() is used to filter rows based on pattern matching

<img width="825" height="629" alt="image" src="https://github.com/user-attachments/assets/c6295a80-2ffb-43cf-aea2-6fa1221376f9" />

# sort():
used to arrange rows in ascending or descending order based on one or more columns.
Sort by salary (ascending)

df.sort("sal").show()

<img width="715" height="603" alt="image" src="https://github.com/user-attachments/assets/a7edf59e-5ce1-4f56-beda-9131ad8abe55" />

# Sort by salary (descending)

df.sort(df.sal.desc()).show()

# Sort by department, then salary (multi-column sort)

df.sort("dept", "sal").show()

# Sort by age descending

df.describe().show()

df.sort(df.age.desc()).show()

# describe()

df.describe("sal", "age").show()

describe() is a DataFrame function that gives summary statistics (basic statistical information) for numeric columns and sometimes string columns.

It returns:

count → number of rows

mean → average

stddev → standard deviation

min → minimum value

max → maximum value

It is similar to DESCRIBE or SUMMARY in SQL.

<img width="875" height="612" alt="image" src="https://github.com/user-attachments/assets/07cf49fd-1a9a-4286-ad0f-6253cd21f913" />

# columns()

A column is a named field in a DataFrame,

columns is NOT a function, it is a property.

So you should NOT use parentheses ().

<img width="871" height="636" alt="image" src="https://github.com/user-attachments/assets/2049e4e1-29bd-4bf9-89c1-bf61d0b582d8" />

# Dataset 2 — String Functions

data = [
    (1, "  alice  ", "HR,Admin", "Alice123"),
    (2, "bob", "IT,Support", "B@b_2025"),
    (3, "  CHARLIE", "Finance,Audit", "Ch@rlie#99"),
    (4, "DAVID  ", "IT,Security", "David!!"),
    (5, "eve", "HR,Training", "EvE2024")
]
cols = ["ID", "Name", "Departments", "Username"]
df = spark.createDataFrame(data, cols)

# upper()
Converts a string to uppercase.

df.select(upper("name")).show()

<img width="959" height="641" alt="image" src="https://github.com/user-attachments/assets/4c9723e8-65f5-4404-bfc1-84bd28513b11" />

# trim():
Removes spaces from both sides.

df.select(trim("name")).show()

<img width="882" height="612" alt="image" src="https://github.com/user-attachments/assets/7bd27045-cafb-453b-a599-2da5d1bc5350" />

# ltrim():
Removes spaces from the left side.

df.select(ltrim("name")).show()

<img width="950" height="636" alt="image" src="https://github.com/user-attachments/assets/fb8a0f62-0b7d-4f2f-87d2-c17e6b202c99" />

# rtrim():
Removes spaces from the right side.

df.select(rtrim("name")).show()

<img width="866" height="638" alt="image" src="https://github.com/user-attachments/assets/585276e2-f432-415a-9dd0-1ee96d5bd7e4" />

# substring_index():
Split the string by a delimiter and return part before/after delimiter.

df.select(substring_index(col("Departments"), ",", 1).alias("Main_Dept")).show()
df.select(substring_index(col("Departments"), ",", -1).alias("Main_Dept")).show()

<img width="972" height="628" alt="image" src="https://github.com/user-attachments/assets/761d73e5-054d-4668-b37e-f6e7cc3eb3ab" />

<img width="999" height="629" alt="image" src="https://github.com/user-attachments/assets/e11f53e7-63f7-4055-b066-d2e174a71748" />


# substring():
Extract characters from specific position.Extracts part of a string based on position.

df.select(substring(col("Username"), 1, 5).alias("Sub_Username")).show()

<img width="957" height="605" alt="image" src="https://github.com/user-attachments/assets/f06e597f-5a2b-41a2-9830-12567249d896" />

# split():
Splits a string into an array.

df.select(split(col("Departments"), ",").alias("Split_Dept")).show(truncate=False)


<img width="913" height="641" alt="image" src="https://github.com/user-attachments/assets/84a2fe51-6cec-499d-98f5-f7abf9c160ac" />

# repeat():
Repeats the string N times.

df.select(repeat(trim(col("Name")), 2).alias("Repeat_Name")).show()

<img width="935" height="631" alt="image" src="https://github.com/user-attachments/assets/b732a40e-da8f-4c48-9d0f-3332bbe1bf30" />

# rpad():
Pad the string on the RIGHT side to a fixed length.

df.select(rpad(trim(col("Name")), 10, "*").alias("Rpad_Name")).show()

<img width="905" height="604" alt="image" src="https://github.com/user-attachments/assets/29891b30-9572-42db-9c37-3eabcdc2cf76" />

# lpad():
Purpose: Pads the string to the left with a specific character.

df.select(lpad(trim(col("Name")), 10, "#").alias("Lpad_Name")).show()

<img width="973" height="628" alt="image" src="https://github.com/user-attachments/assets/30bb9d17-a763-4162-8383-fb7db9d5bd78" />

# regex_replace():

Purpose: Replaces matching substrings using a regular expression.

df.select(regexp_replace(col("Username"), "[^a-zA-Z]", "").alias("Clean_Username")).show()

<img width="964" height="639" alt="image" src="https://github.com/user-attachments/assets/ba59db28-d176-4340-9bf4-2b397b45601d" />

# lower():

Purpose: Converts string to lowercase.

df.select(lower(col("Departments")).alias("Lower_Dept")).show()

<img width="839" height="631" alt="image" src="https://github.com/user-attachments/assets/dc87107d-a27f-4023-91a9-098795242eb4" />

# regexp_extract()

Purpose: Extracts substring using a regular expression.

df.select(regexp_extract(col("Username"), r"[A-Za-z]+", 0).alias("Extracted_Text")).show()

<img width="990" height="620" alt="image" src="https://github.com/user-attachments/assets/a1688e4e-5081-4840-86a2-4c6ca60f3b64" />

# length()

Purpose: Returns length of the string.

df.select(length(trim(col("Name"))).alias("Name_Length")).show()

<img width="887" height="633" alt="image" src="https://github.com/user-attachments/assets/df9c26a2-d13b-45ac-b281-2cab6f3be351" />

# instr()

Purpose: Returns the position of substring.

df.select(instr(col("Departments"), "IT").alias("IT_Position")).show()

<img width="955" height="622" alt="image" src="https://github.com/user-attachments/assets/e7bdc064-a834-4a53-86bf-60464f6d2669" />

# initcap()

Purpose: Converts the first letter of each word to uppercase.

df.select(initcap(lower(col("Departments"))).alias("Initcap_Dept")).show()

<img width="944" height="612" alt="image" src="https://github.com/user-attachments/assets/61ca2d9d-04dc-4682-a199-ada1f36270e1" />

# 15. Numeric Functions

| **Function** | **Purpose**                                       | **Example**                                       |
| ------------ | ------------------------------------------------- | ------------------------------------------------- |
| `SUM()`      | Calculates the total sum of a numeric column      | `SUM(salary)` → total salary                      |
| `AVG()`      | Calculates the average (mean) of a numeric column | `AVG(salary)` → average salary                    |
| `MIN()`      | Returns the minimum value in a numeric column     | `MIN(salary)` → smallest salary                   |
| `MAX()`      | Returns the maximum value in a numeric column     | `MAX(salary)` → largest salary                    |
| `ROUND()`    | Rounds numeric values to specified decimal places | `ROUND(salary, 2)` → salary rounded to 2 decimals |
| `ABS()`      | Returns absolute value of a number                | `ABS(-500)` → 500                                 |

# 15.1 SUM():

df.agg(sum("Salary").alias("Total_Salary")).show()


<img width="850" height="606" alt="image" src="https://github.com/user-attachments/assets/61258442-3a22-43f5-9ec9-92b5591298d8" />

# AVG()

df.agg(avg("Salary").alias("Avg_Salary")).show()

<img width="958" height="639" alt="image" src="https://github.com/user-attachments/assets/e6b7ede8-f154-479d-9c35-d0e371768c01" />

# MIN()

df.agg(min("Salary").alias("Min_Salary")).show()

<img width="996" height="637" alt="image" src="https://github.com/user-attachments/assets/96ef34c1-bb3b-41d5-8814-71c886ad8a30" />

# MAX()

df.agg(max("Salary").alias("Max_Salary")).show()

<img width="847" height="638" alt="image" src="https://github.com/user-attachments/assets/46352595-7745-4817-9587-48518baa3349" />

# ROUND()

df.select(round("Salary", -3).alias("Rounded_Salary")).show()

<img width="856" height="640" alt="image" src="https://github.com/user-attachments/assets/0fec68fc-e9a3-4319-8e9e-c384be6f5198" />

# ABS()

df.select(abs(col("Salary") - 5500).alias("Abs_Diff")).show()

<img width="972" height="645" alt="image" src="https://github.com/user-attachments/assets/2a55aeaf-3f51-403b-9592-87a0609df0c0" />

# 16. Date and Time Functions: 

| Function              | Purpose                              |
| --------------------- | ------------------------------------ |
| `CURRENT_DATE()`      | Returns current date                 |
| `CURRENT_TIMESTAMP()` | Returns current date & time          |
| `DATE_ADD()`          | Adds days to a date                  |
| `DATEDIFF()`          | Difference between two dates in days |
| `YEAR()`              | Extracts year                        |
| `MONTH()`             | Extracts month                       |
| `DAY()`               | Extracts day                         |
| `TO_DATE()`           | Converts string/timestamp to date    |
| `DATE_FORMAT()`       | Formats date as string               |


# 16.1 CURRENT_DATE()

df.select(current_date().alias("today_date")).show()

<img width="811" height="643" alt="image" src="https://github.com/user-attachments/assets/f481086c-71ed-4ef2-b998-b82b65fb848e" />

# CURRENT_TIMESTAMP()

df.select(current_timestamp().alias("current_time")).show()

<img width="950" height="642" alt="image" src="https://github.com/user-attachments/assets/cd5ef0a9-2489-41bd-9b44-d6472961e565" />

# DATE_ADD()

df.select(date_add(current_date(), 7).alias("after_7_days")).show()

<img width="724" height="646" alt="image" src="https://github.com/user-attachments/assets/abbce7b1-4550-42c9-9a20-ba43325547ec" />

# DATEDIFF()

df.select(datediff(current_date(), "2025-01-01").alias("days_diff")).show()

df.select(datediff(current_date(), lit("2025-01-01")).alias("days_diff")).show()

<img width="970" height="646" alt="image" src="https://github.com/user-attachments/assets/c36a8d31-1d40-4dcc-b9df-17bbd4f664e7" />

# YEAR()
df.select(year(current_date()).alias("year")).show()

<img width="890" height="637" alt="image" src="https://github.com/user-attachments/assets/fba3a050-af50-4668-b660-836d07acc4d0" />

# MONTH() and DAY() 

df.select(month(current_date()).alias("month")).show()

df.select(dayofmonth(current_date()).alias("day")).show()

<img width="879" height="639" alt="image" src="https://github.com/user-attachments/assets/1374d12e-8021-48e9-aecd-02d0acc69df5" />

<img width="872" height="633" alt="image" src="https://github.com/user-attachments/assets/abb82da6-8b8b-43c0-8e3f-44df0970a0b7" />

<img width="830" height="635" alt="image" src="https://github.com/user-attachments/assets/b728f97f-6f78-455e-8edb-528c8762cbc0" />


# TO_DATE()

df.select(to_date("2025-12-15").alias("converted_date")).show()

df.select(to_date(lit("2025-12-15")).alias("converted_date")).show()

<img width="737" height="639" alt="image" src="https://github.com/user-attachments/assets/a5ab376b-ee78-462e-8a31-6fcb8f99d000" />

# DATE_FORMAT()

df.select(date_format(current_date(), "dd-MMM-yyyy").alias("formatted_date")).show()


<img width="873" height="640" alt="image" src="https://github.com/user-attachments/assets/666b5ed0-1d2c-468c-bb01-4b7b2c2e0b51" />

# 17. Aggregate Functions
# 17.1 mean()

from pyspark.sql.functions import mean, avg, round

<img width="933" height="604" alt="image" src="https://github.com/user-attachments/assets/3e3f3f58-2aec-4c21-8acc-8921d7095966" />

# 17.2 avg()

df.agg(avg("salary").alias("avg_salary")).show()

<img width="769" height="605" alt="image" src="https://github.com/user-attachments/assets/597df3d2-cd51-47b8-9400-621620ba0e3e" />

# 3.3 collect_list()

df.groupBy("departments").agg(collect_list("Name").alias("dept_vise_names")).show()

<img width="998" height="631" alt="image" src="https://github.com/user-attachments/assets/11811803-aa8d-478e-8bee-0df305990066" />

# 17.4 collect_set(): 

df.groupBy("dept").agg(collect_set("reg").alias("dept_vise_reg")).show()

<img width="884" height="634" alt="image" src="https://github.com/user-attachments/assets/5a90b6a9-7a0c-48bc-abb7-b067bdfeeb72" />

# 17.5 countDistinct()

df.groupBy("dept").agg(count_distinct("Name").alias("dept_vise_name_count")).show()

<img width="940" height="639" alt="image" src="https://github.com/user-attachments/assets/be12fc0a-ae83-42a1-be63-d8d795b2b579" />

# count()

df.groupBy("departments").agg(count("Name").alias("dept_wise_count")).show()
<img width="940" height="639" alt="image" src="https://github.com/user-attachments/assets/be12fc0a-ae83-42a1-be63-d8d795b2b579" />

# first()
df1=df.groupBy("departments").agg(first("Name").alias("dept_wise_firstname")).show()

<img width="782" height="606" alt="image" src="https://github.com/user-attachments/assets/a34c180b-cef8-4592-b342-6f213b0c2ee7" />

# last()
df1=df.groupBy("departments").agg(last("Name").alias("dept_wise_lastname")).show()

<img width="796" height="637" alt="image" src="https://github.com/user-attachments/assets/7d76b70e-affc-454d-b135-c3133a915236" />

# max(), 3.10 min(), 3.11 sum()

df.agg(max("salary")).alias("max_sal").show()

df.agg(min("salary")).alias("min_sal").show()

df.agg(sum("salary")).alias("total_sal").show()

df.agg(max("sal").alias("max_sal"), min("sal").alias("min_sal"), sum("sal").alias("total_sal")).show()

<img width="1009" height="641" alt="image" src="https://github.com/user-attachments/assets/9ad9e218-04c8-4c98-ba93-345ed21514b1" />

# 18. Joins: 
JOIN is used to combine data from two or more tables

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("practice").getOrCreate()

data_emp = [
    (1, "Aarav", 101, 45000.75),
    (2, "Isha", 102, 60000.20),
    (3, "Rohan", 101, 52000.60),
    (4, "Meera", 103, 48000.40),
    (5, "Arjun", 104, 75000.00),
    (6, "Priya", 101, 50000.00),
    (7, "Kiran", 102, 65000.90),
    (8, "Diya", 105, 40000.50),
    (9, "Manav", 106, 39000.25),
    (10, "Sneha", 107, 47000.00),
    (11, "Varun", 103, 58000.35),
    (12, "Asha", 104, 72000.10),
    (13, "Vivek", 105, 51000.00),
    (14, "Riya", 106, 43000.70),
    (15, "Neel", 108, 62000.55)
]

data_dept = [
    (101, "HR", "Mumbai"),
    (102, "IT", "Bangalore"),
    (103, "Finance", "Pune"),
    (104, "Marketing", "Delhi"),
    (105, "Operations", "Chennai"),
    (106, "Admin", "Hyderabad")
]

columns_emp = ["emp_id", "name", "dept_id", "salary"]

columns_dept = ["dept_id", "dept_name", "location"]

dF= spark.createDataFrame(data_emp, columns_emp)

df1 = spark.createDataFrame(data_dept, columns_dept)

# 18.1 inner join: 

df.join(df1, "dept_id", "inner").show()

<img width="668" height="658" alt="image" src="https://github.com/user-attachments/assets/c5be34f2-942d-4e70-ba3d-44e7597e599e" />











































































