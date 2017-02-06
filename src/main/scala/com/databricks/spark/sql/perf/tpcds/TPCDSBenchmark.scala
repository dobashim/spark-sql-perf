package com.databricks.spark.sql.perf.tpcds

import com.databricks.spark.sql.perf.{Benchmark, Query}
import org.apache.log4j.LogManager
import org.apache.spark.sql.{SQLContext, SparkSession}

import scala.collection.mutable

class TPCDSBenchmark(sqlContext: SQLContext)
  extends Benchmark(sqlContext)
  with Tpcds_1_4_Queries
  with ImpalaKitQueries
  with SimpleQueries
  with Serializable {

  /**
    * Simple utilities to run the queries without persisting the results.
    */
  def explain(queries: Seq[Query], showPlan: Boolean = false): Unit = {
    val succeeded = mutable.ArrayBuffer.empty[String]
    queries.foreach { q =>
      println(s"Query: ${q.name}")
      try {
        val df = sqlContext.sql(q.sqlText.get)
        if (showPlan) {
          df.explain()
        } else {
          df.queryExecution.executedPlan
        }
        succeeded += q.name
      } catch {
        case e: Exception =>
          println("Failed to plan: " + e)
      }
    }
    println(s"Planned ${succeeded.size} out of ${queries.size}")
    println(succeeded.map("\"" + _ + "\""))
  }

  def run(queries: Seq[Query], numRows: Int = 1, timeout: Int = 0): Unit = {
    val succeeded = mutable.ArrayBuffer.empty[String]
    queries.foreach { q =>
      println(s"Query: ${q.name}")
      val start = System.currentTimeMillis()
      val df = sqlContext.sql(q.sqlText.get)
      var failed = false
      val jobgroup = s"benchmark ${q.name}"
      val t = new Thread("query runner") {
        override def run(): Unit = {
          try {
            sqlContext.sparkContext.setJobGroup(jobgroup, jobgroup, true)
            df.show(numRows)
          } catch {
            case e: Exception =>
              println("Failed to run: " + e)
              failed = true
          }
        }
      }
      t.setDaemon(true)
      t.start()
      t.join(timeout)
      if (t.isAlive) {
        println(s"Timeout after $timeout seconds")
        sqlContext.sparkContext.cancelJobGroup(jobgroup)
        t.interrupt()
      } else {
        if (!failed) {
          succeeded += q.name
          println(s"   Took: ${System.currentTimeMillis() - start} ms")
          println("------------------------------------------------------------------")
        }
      }
    }
    println(s"Ran ${succeeded.size} out of ${queries.size}")
    println(succeeded.map("\"" + _ + "\""))
  }
}

/**
  * Created by dobachi on 2017/02/06.
  */
object TPCDSBenchmark {
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

        val tpcDSBenchmark = new TPCDSBenchmark()
        tpcDSBenchmark.explain(tpcDSBenchmark.interactiveQueries)

      case None =>
        sys.exit(1)
    }
  }

}
