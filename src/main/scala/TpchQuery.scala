package main.scala

import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ListBuffer
import scala.io.Source

import scopt.OptionParser
/**
 * Parent class for TPC-H queries.
 *
 * Defines schemas for tables and reads pipe ("|") separated text files into these tables.
 *
 * Savvas Savvides <savvas@purdue.edu>
 *
 */


abstract class TpchQuery{

  // get the name of the class excluding dollar signs and package
  private def escapeClassName(className: String): String = {
    className.split("\\.").last.replaceAll("\\$", "")
  }

  def getName(): String = escapeClassName(this.getClass.getName)

  /**
   *  implemented in children classes and hold the actual query
   */
  def execute(sc: SparkContext, tpchSchemaProvider: TpchSchemaProvider): DataFrame

  def getRawSQL(): String
}

object TpchQuery  extends Logging{

  var IP : String = Source.fromFile("/home/ec2-user/hadoop/conf/masters").getLines.toList.head

  def outputDF(df: DataFrame, outputDir: String, className: String): Unit = {
    df.collect().take(10).foreach(println)
  }

  def executeQueries(sc: SparkContext, inputDir: String, queryNum: Int): ListBuffer[(String, Float)] = {
    val OUTPUT_DIR: String = "file://" + new File(".").getAbsolutePath() + "/tpch_out"

    val results = new ListBuffer[(String, Float)]

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    var queryNo = queryNum
    val query = Class.forName(f"main.scala.Q${queryNo}%02d").newInstance.asInstanceOf[TpchQuery]

    val schemaProvider = new TpchSchemaProvider(sc, inputDir)

    val beginTime = System.nanoTime()

    outputDF(query.execute(sc, schemaProvider), OUTPUT_DIR, query.getName())

    val timeSingleElapsed = (System.nanoTime() - beginTime)/1000000.0f // milisecond
    logInfo(s"End executing query. Time: ${timeSingleElapsed}")
    sqlContext.clearCache()

    results += new Tuple2(query.getName(), timeSingleElapsed)

    return results
  }

  def runParquetQueries(spark: SparkSession, inputDir: String, queryNum: Int): Unit = {
    // Encoders for most common types are automatically provided by importing spark.implicits._
    import spark.implicits._

    val customerFileDF = spark.read.parquet(inputDir + "/customer.parquet")
    val lineitemFileDF = spark.read.parquet(inputDir + "/lineitem.parquet")
    val nationFileDF = spark.read.parquet(inputDir + "/nation.parquet")
    val regionFileDF = spark.read.parquet(inputDir + "/region.parquet")
    val ordersFileDF = spark.read.parquet(inputDir + "/orders.parquet")
    val partFileDF = spark.read.parquet(inputDir + "/part.parquet")
    val partsuppFileDF = spark.read.parquet(inputDir + "/partsupp.parquet")
    val supplierFileDF = spark.read.parquet(inputDir + "/supplier.parquet")

    customerFileDF.createOrReplaceTempView("customer")
    lineitemFileDF.createOrReplaceTempView("lineitem")
    nationFileDF.createOrReplaceTempView("nation")
    regionFileDF.createOrReplaceTempView("region")
    ordersFileDF.createOrReplaceTempView("orders")
    partFileDF.createOrReplaceTempView("part")
    partsuppFileDF.createOrReplaceTempView("partsupp")
    supplierFileDF.createOrReplaceTempView("supplier")

    val query = Class.forName(f"main.scala.Q${queryNum}%02d").newInstance.asInstanceOf[TpchQuery].getRawSQL()

    val beginTime = System.nanoTime()
    val resultDF = spark.sql(query)
    resultDF.show()

    val timeSingleElapsed = (System.nanoTime() - beginTime)/1000000.0f // milisecond
    logInfo(s"End executing query. Time: ${timeSingleElapsed}")
    spark.catalog.clearCache()

  }

  case class CommandLineArgs(
    queryNum: Int = 1,
    exeQuery: Boolean = false,
    convertTable: Boolean = false,
    appName: String = "TPCH Query in 2 workers",
    runParquet: Boolean = false,
    fromHDFS: Boolean = false,
    logTrace: Boolean = false
  )

  def main(args: Array[String]): Unit = {
    
    val parser = new scopt.OptionParser[CommandLineArgs]("Column-Cache Experiment") {
      head("scopt", "3.7.1")
      
      opt[Int]('q', "query")
        .action ((x, c) =>
            c.copy(queryNum = x, exeQuery = true)
          )
        .text ("query is num of the query to be excecuted")

      opt[Unit]('c', "convert-table")
        .action ( (_, c) =>
            c.copy(convertTable = true)
          )
        .text ("with this a task to convert tbl to parquet would be excecuted")

      opt[Unit]('p', "run-parquet")
        .action ( (_, c) =>
            c.copy(runParquet = true)
          )
        .text ("with this queries would be excecuted on parquet files")

      help("help").text("print this usage text.")

      opt[String]('n', "app-name")
        .action ( (x, c) =>
              c.copy(appName = x)
          )
        .text ("spark application name")

      opt[Unit]('f', "from-hdfs")
        .action ( (_, c) =>
            c.copy(fromHDFS = true)
          )
        .text ("with this a task read tbl from hdfs")

      opt[Unit]('t', "log-trace")
        .action ( (_, c) =>
            c.copy(logTrace = true)
          )
        .text ("set logger in TRACE level")
    }

    var input_prefix: String = ""

    parser.parse(args, CommandLineArgs()) match {
      case Some(config) =>
      // do stuff
      {
        run(config)
      }
      case None =>
      // arguments are bad, error message will have been displayed
    }


    def run(params: CommandLineArgs): Unit = {
      if(params.fromHDFS){
        input_prefix = s"hdfs://${IP}:9000/home/ec2-user"
      }
      else{
        input_prefix = s"alluxio://${IP}:19998/home/ec2-user"
      }
      if(params.convertTable){
        val ct = new ConvertTable()
        ct.parseTable(input_prefix)
      }

      if(params.exeQuery && !params.runParquet){
        // val conf = new SparkConf().setAppName(params.appName)
        // // read files from local FS
        // // val INPUT_DIR = "file://" + new File(".").getAbsolutePath() + "/dbgen"

        // // read from hdfs
        // // val INPUT_DIR: String = "/dbgen"

        // // read from alluxio
        // val INPUT_DIR = s"alluxio://${IP}:19998/home/ec2-user/data"

        // val output = new ListBuffer[(String, Float)]
        // output ++= executeQueries(conf, INPUT_DIR, params.queryNum)
        val conf = new SparkConf().setAppName(params.appName)

        val sc = new SparkContext(conf)
        if(params.logTrace){
          sc.setLogLevel("TRACE")
        }
        logInfo(s"Got application ID: ${sc.applicationId}")

        val INPUT_DIR = s"alluxio://${IP}:19998/home/ec2-user/data"

        val output = new ListBuffer[(String, Float)]
        output ++= executeQueries(sc, INPUT_DIR, params.queryNum)
        
      }
      else if(params.exeQuery && params.runParquet){
        val sparksession = SparkSession
          .builder()
          .appName(params.appName)
          .getOrCreate()

        if(params.logTrace){
          sparksession.sparkContext.setLogLevel("TRACE")
        }

        logInfo(s"Got application ID: ${sparksession.sparkContext.applicationId}")
        logInfo(s"Run query: ${params.queryNum}")

        val INPUT_DIR = s"alluxio://${IP}:19998/home/ec2-user/tpch_parquet"
        runParquetQueries(sparksession, INPUT_DIR, params.queryNum)
      }

    }
  }
}
