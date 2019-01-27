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
    df.collect().foreach(println)

//    if (outputDir == null || outputDir == "")
//      df.collect().foreach(println)
//    else
//      //df.write.mode("overwrite").json(outputDir + "/" + className + ".out") // json to avoid alias
//      df.write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").save(outputDir + "/" + className)
  }

  def executeQueries(conf: SparkConf, inputDir: String, queryNum: Int): ListBuffer[(String, Float)] = {

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

    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val totalTime = Source.fromFile("/home/ec2-user/tpch-spark/times").getLines.toList.head.toInt
    for (queryNo <- fromNum to toNum) {
      val query = Class.forName(f"main.scala.Q${queryNo}%02d").newInstance.asInstanceOf[TpchQuery]
//      outputDF(query.execute(sc, schemaProvider), OUTPUT_DIR, query.getName())

      val t0 = System.nanoTime()

      logInfo(s"Run query ${queryNo} ${totalTime} times")

      val t2 = System.nanoTime()

      for (t <- 1 to totalTime) {
//        val sc = SparkContext.getOrCreate(conf)

        sqlContext.clearCache()
        val t4 = System.nanoTime()
        logInfo(s"Begin at ${t} round")

        val schemaProvider = new TpchSchemaProvider(sc, inputDir)
        logInfo(s"Generate schema at ${t} round")

        outputDF(query.execute(sc, schemaProvider), OUTPUT_DIR, query.getName())
        val timeSingleElapsed = (System.nanoTime() - t4)/1000000.0f // milisecond
        logInfo(s"Time elapsed at ${t} round: ${timeSingleElapsed}")

//        sc.stop()
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
//    conf.set("spark.sql.crossJoin.enabled", "true")
    conf.set("spark.driver.maxResultSize", "4G")
//    val sc = new SparkContext(conf)

    // read files from local FS
    // val INPUT_DIR = "file://" + new File(".").getAbsolutePath() + "/dbgen"

    // read from hdfs
    // val INPUT_DIR: String = "/dbgen"

    // read from alluxio
    val INPUT_DIR = s"alluxio://${IP}:19998/tpch"
//    logInfo(s"Input dir : ${INPUT_DIR}")

    val output = new ListBuffer[(String, Float)]
    output ++= executeQueries(conf, INPUT_DIR, queryNum)

    val outFile = new File("TIMES.txt")
    val bw = new BufferedWriter(new FileWriter(outFile, true))

    output.foreach {
      case (key, value) => bw.write(f"${key}%s\t${value}%1.8f\n")
    }

    bw.close()
  }
}
