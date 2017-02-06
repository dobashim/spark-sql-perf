# Spark SQL Performance Tests

[![Build Status](https://travis-ci.org/databricks/spark-sql-perf.svg)](https://travis-ci.org/databricks/spark-sql-perf)

This is a performance testing framework for [Spark SQL](https://spark.apache.org/sql/) in [Apache Spark](https://spark.apache.org/) 1.6+.

**Note: This README is still under development. Please also check our source code for more information.**

# Quick Start

```
$ bin/run --help

spark-sql-perf 0.2.0
Usage: spark-sql-perf [options]

  -b <value> | --benchmark <value>
        the name of the benchmark to run
  -f <value> | --filter <value>
        a filter on the name of the queries to run
  -i <value> | --iterations <value>
        the number of iterations to run
  --help
        prints this usage text
        
$ bin/run --benchmark DatasetPerformance
```

# TPC-DS

## How to use it
The rest of document will use TPC-DS benchmark as an example. We will add contents to explain how to use other benchmarks add the support of a new benchmark dataset in future.

### Setup a benchmark
Before running any query, a dataset needs to be setup by creating a `Benchmark` object. Generating
the TPCDS data requires dsdgen built and available on the machines. We have a fork of dsdgen that
you will need. It can be found [here](https://github.com/davies/tpcds-kit).  

```
import com.databricks.spark.sql.perf.tpcds.Tables
// Tables in TPC-DS benchmark used by experiments.
// dsdgenDir is the location of dsdgen tool installed in your machines.
val tables = new Tables(sqlContext, dsdgenDir, scaleFactor)
// Generate data.
tables.genData(location, format, overwrite, partitionTables, useDoubleForDecimal, clusterByPartitionColumns, filterOutNullPartitionValues)
// Create metastore tables in a specified database for your data.
// Once tables are created, the current database will be switched to the specified database.
tables.createExternalTables(location, format, databaseName, overwrite)
// Or, if you want to create temporary tables
tables.createTemporaryTables(location, format)
// Setup TPC-DS experiment
import com.databricks.spark.sql.perf.tpcds.TPCDS
val tpcds = new TPCDS (sqlContext = sqlContext)
```

### Setup a benchmark data by GenData application

You can use GenData application to generate data.
The following is an example of execution with Spark on YARN.

```
$ spark-submit --class com.databricks.spark.sql.perf.tpcds.GenData\
target/scala-2.10/spark-sql-perf-assembly-0.4.11-SNAPSHOT.jar\
/usr/local/tpc-ds/default/tools\
1\
/tmp/dsdgen\
--sparkMaster yarn
```

Help

```
$ spark-submit --class com.databricks.spark.sql.perf.tpcds.GenData target/scala-2.10/spark-sql-perf-assembly-0.4.11-SNAPSHOT.jar

Error: Missing argument dsdgenDir
Error: Missing argument scaleFactor
Error: Missing argument location
GenData
Usage: gendata [options] dsdgenDir scaleFactor location

  dsdgenDir
        The path to the directory which includes dsdgen tool
  scaleFactor
        The scale factor to be provided to dsdgen tool
  location
        The path to the output directory
  --format <value>
        The output format, such as parquet, csv and so on. default: parquet
  --sparkMaster <value>
        The master url of Spark. default: local
  --databaseName <value>
        The name of the output database. default: tpcds
  --overwrite <value>
        Whether to enable overwrite mode. default: true
  --partitionTables <value>
        Whether to split tables to partitions. default: true
  --useDoubleForDecimal <value>
        Whether to use Double type as a representation of Decimal. default: true
  --clusterBypartitionColumns <value>
        Whether to split tables by any columns. default: true
  --filterOutNullPartitionValues <value>
        Whether to filter the output partitions which include null. default: true
```

### Run benchmarking queries
After setup, users can use `runExperiment` function to run benchmarking queries and record query execution time. Taking TPC-DS as an example, you can start an experiment by using

```
val experiment = tpcds.runExperiment(tpcds.interactiveQueries)
```

For every experiment run (i.e. every call of `runExperiment`), Spark SQL Perf will use the timestamp of the start time to identify this experiment. Performance results will be stored in the sub-dir named by the timestamp in the given `resultsLocation` (for example `results/1429213883272`). The performance results are stored in the JSON format.

### Retrieve results
While the experiment is running you can use `experiment.html` to list the status.  Once the experiment is complete, the results will be saved to the table sqlPerformance in json.

```
// Get all experiments results.
tpcds.createResultsTable()
sqlContext.table("sqlPerformance")
// Get the result of a particular run by specifying the timestamp of that run.
sqlContext.table("sqlPerformance").filter("timestamp = 1429132621024")
```
