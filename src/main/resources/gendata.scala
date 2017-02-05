import com.databricks.spark.sql.perf.tpcds.Tables

// Tables in TPC-DS benchmark used by experiments.
// dsdgenDir is the location of dsdgen tool installed in your machines.
val dsdgenDir = "/usr/local/tpc-ds/default/tools"
val scaleFactor = 10
val sqlContext = spark.sqlContext
val tables = new Tables(sqlContext, dsdgenDir, scaleFactor)

// Generate data.
val location = "/tmp/dsdgen"
val format = "text"
val overwrite = true
val partitionTables = true
val useDoubleForDecimal = true
val clusterByPartitionColumns = true
val filterOutNullPartitionValues = true
tables.genData(location, format, overwrite, partitionTables, useDoubleForDecimal, clusterByPartitionColumns, filterOutNullPartitionValues)

// Create metastore tables in a specified database for your data.
// Once tables are created, the current database will be switched to the specified database.
val databaseName = "tpcds"
tables.createExternalTables(location, format, databaseName, overwrite)

// Or, if you want to create temporary tables
tables.createTemporaryTables(location, format)

// Setup TPC-DS experiment
import com.databricks.spark.sql.perf.tpcds.TPCDS
val tpcds = new TPCDS (sqlContext = sqlContext)
