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

    if (outputDir == null || outputDir == "")
      df.collect().foreach(println)
    else
      //df.write.mode("overwrite").json(outputDir + "/" + className + ".out") // json to avoid alias
      df.write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").save(outputDir + "/" + className)
  }

  def executeQueries(sc: SparkContext, schemaProvider: TpchSchemaProvider, queryNum: Int): ListBuffer[(String, Float)] = {

    // if set write results to hdfs, if null write to stdout
    // val OUTPUT_DIR: String = "/tpch"
    // val OUTPUT_DIR: String = "file://" + new File(".").getAbsolutePath() + "/output"

    val OUTPUT_DIR = s"alluxio://${IP}:19998/tpch_out"
//    logInfo(s"Output dir : ${OUTPUT_DIR}")


    val results = new ListBuffer[(String, Float)]

    var fromNum = 1;
    var toNum = 22;
    if (queryNum != 0) {
      fromNum = queryNum;
      toNum = queryNum;
    }

    val totalTime = 5
    for (queryNo <- fromNum to toNum) {
      val t0 = System.nanoTime()

      val query = Class.forName(f"main.scala.Q${queryNo}%02d").newInstance.asInstanceOf[TpchQuery]
   
      logInfo(s"Run query ${queryNo} ${totalTime} times")

//      outputDF(query.execute(sc, schemaProvider), OUTPUT_DIR, query.getName())
      val t2 = System.nanoTime()

      for (t <- 1 to totalTime) {
        logInfo(s"Run the ${t}th time")
        outputDF(query.execute(sc, schemaProvider), OUTPUT_DIR, query.getName())
//        query.execute(sc, schemaProvider)
      }
      val t3 = System.nanoTime()
      val timeElapsed = (t3 - t2) / 1000000.0f // milisecond

      logInfo(s"Finish running query ${queryNo}. Time : ${timeElapsed}")

      val t1 = System.nanoTime()

      val elapsed = (t1 - t0) / 1000000000.0f // second
      results += new Tuple2(query.getName(), elapsed)

    }
    return results
  }

  def main(args: Array[String]): Unit = {

    var queryNum = 0;
    if (args.length > 0)
      queryNum = args(0).toInt

    val conf = new SparkConf().setAppName("TPCH Query in 2 workers")
    val sc = new SparkContext(conf)

    // read files from local FS
    // val INPUT_DIR = "file://" + new File(".").getAbsolutePath() + "/dbgen"

    // read from hdfs
    // val INPUT_DIR: String = "/dbgen"

    // read from alluxio
    val INPUT_DIR = s"alluxio://${IP}:19998/tpch"
//    logInfo(s"Input dir : ${INPUT_DIR}")

    val schemaProvider = new TpchSchemaProvider(sc, INPUT_DIR)

    val output = new ListBuffer[(String, Float)]
    output ++= executeQueries(sc, schemaProvider, queryNum)

    val outFile = new File("TIMES.txt")
    val bw = new BufferedWriter(new FileWriter(outFile, true))

    output.foreach {
      case (key, value) => bw.write(f"${key}%s\t${value}%1.8f\n")
    }

    bw.close()
  }
}
