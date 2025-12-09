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
 # create SparkContext
  from pyspark.sql import SQLContext
  
sqlContext = SQLContext(sc)
# 4.2 Configuring and creating a SparkSession.

