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









