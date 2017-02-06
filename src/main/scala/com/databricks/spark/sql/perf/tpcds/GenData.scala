package com.databricks.spark.sql.perf.tpcds

import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession

/**
  * Created by dobachi on 2017/02/06.
  */
object GenData {
  def main(args: Array[String]): Unit = {

    val log = LogManager.getRootLogger

    // Scopt configuration
    case class Config(dsdgenDir: String = "path-to-tpcds-tools",
                      scaleFactor: Int = 10,
                      location: String = "/tmp/dsdgen",
                      format: String = "parquet",
                      sparkMaster: String = "local",
                      databaseName: String = "tpcds",
                      overwrite: Boolean = true,
                      partitionTables: Boolean = true,
                      useDoubleForDecimal: Boolean = true,
                      clusterBypartitionColumns: Boolean = true,
                      filterOutNullPartitionValues: Boolean = true)

    val parser = new scopt.OptionParser[Config]("gendata") {
      head("GenData")

      arg[String]("dsdgenDir").action((x, c) =>
       c.copy(dsdgenDir = x)).text("The path to the directory which includes dsdgen tool")

      arg[Int]("scaleFactor").action((x, c) =>
        c.copy(scaleFactor = x)).text("The scale factor to be provided to dsdgen tool")

      arg[String]("location").action((x, c) =>
        c.copy(location= x)).text("The path to the output directory")

      opt[String]("format").action((x, c) =>
        c.copy(format = x)).text("The output format, such as parquet, csv and so on. default: parquet")

      opt[String]("sparkMaster").action((x, c) =>
        c.copy(sparkMaster = x)).text("The master url of Spark. default: local")

      opt[String]("databaseName").action((x, c) =>
        c.copy(databaseName = x)).text("The name of the output database. default: tpcds")

      opt[Boolean]("overwrite").action((x, c) =>
        c.copy(overwrite = x)).text("Whether to enable overwrite mode. default: true")

      opt[Boolean]("partitionTables").action((x, c) =>
        c.copy(partitionTables = x)).text("Whether to split tables to partitions. default: true")

      opt[Boolean]("useDoubleForDecimal").action((x, c) =>
        c.copy(useDoubleForDecimal = x)).text("Whether to use Double type as a representation of Decimal. default: true")

      opt[Boolean]("clusterBypartitionColumns").action((x, c) =>
        c.copy(clusterBypartitionColumns = x)).text("Whether to split tables by any columns. default: true")

      opt[Boolean]("filterOutNullPartitionValues").action((x, c) =>
        c.copy(filterOutNullPartitionValues = x)).text("Whether to filter the output partitions which include null. default: true")
    }

    parser.parse(args, Config()) match {
      case Some(config) =>

        // Tables in TPC-DS benchmark used by experiments.
        // dsdgenDir is the location of dsdgen tool installed in your machines.
        val spark = SparkSession.builder().master(config.sparkMaster).appName("TPCDSGenData").getOrCreate()
        val sqlContext = spark.sqlContext
        val tables = new Tables(sqlContext, config.dsdgenDir, config.scaleFactor)

        // Generate data.
        tables.genData(config.location, config.format, config.overwrite,
          config.partitionTables, config.useDoubleForDecimal,
          config.clusterBypartitionColumns, config.filterOutNullPartitionValues)

        // Create metastore tables in a specified database for your data.
        // Once tables are created, the current database will be switched to the specified database.
        tables.createExternalTables(config.location, config.format, config.databaseName, config.overwrite)

        val externalTables = sqlContext.tableNames(config.databaseName)

        log.info("Created tables")
        externalTables.foreach( name =>
          log.info(s"- $name")
        )

      case None =>
        sys.exit(1)
    }
  }

}
