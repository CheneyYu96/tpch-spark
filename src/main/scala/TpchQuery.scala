package main.scala

import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter
import org.apache.spark.sql._
import scala.collection.mutable.ListBuffer
import scala.io.Source

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
}

object TpchQuery  extends Logging{

  var IP : String = Source.fromFile("/home/ec2-user/hadoop/conf/masters").getLines.toList.head


  def outputDF(df: DataFrame, outputDir: String, className: String): Unit = {
    df.collect().take(10).foreach(println)

//    if (outputDir == null || outputDir == "")
//      df.collect().foreach(println)
//    else
//      //df.write.mode("overwrite").json(outputDir + "/" + className + ".out") // json to avoid alias
//      df.write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").save(outputDir + "/" + className)
  }

  def executeQueries(conf: SparkConf, inputDir: String, queryNum: Int): ListBuffer[(String, Float)] = {
    val OUTPUT_DIR: String = "file://" + new File(".").getAbsolutePath() + "/tpch_out"

    val results = new ListBuffer[(String, Float)]

    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    var queryNo = queryNum
    val query = Class.forName(f"main.scala.Q${queryNo}%02d").newInstance.asInstanceOf[TpchQuery]

    val schemaProvider = new TpchSchemaProvider(sc, inputDir)

    val beginTime = System.nanoTime()

    outputDF(query.execute(sc, schemaProvider), OUTPUT_DIR, query.getName())

    val timeSingleElapsed = (System.nanoTime() - beginTime)/1000000.0f // milisecond
    logInfo(s"End executing query. Time: ${timeSingleElapsed}")

    results += new Tuple2(query.getName(), timeSingleElapsed)

    return results
  }

  def main(args: Array[String]): Unit = {

    var queryNum = 0
    var appName = "TPCH Query in 2 workers"

    if (args.length > 0)
      queryNum = args(0).toInt
    if (args.length > 1)
      appName = args(1)

    val conf = new SparkConf().setAppName(appName)
    // read files from local FS
    // val INPUT_DIR = "file://" + new File(".").getAbsolutePath() + "/dbgen"

    // read from hdfs
    // val INPUT_DIR: String = "/dbgen"

    // read from alluxio
    val INPUT_DIR = s"alluxio://${IP}:19998/home/ec2-user/data"

    val output = new ListBuffer[(String, Float)]
    output ++= executeQueries(conf, INPUT_DIR, queryNum)

  }
}
