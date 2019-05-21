package main.scala

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._

import scala.io.Source

class ConvertTable{

  var IP : String = Source.fromFile("/home/ec2-user/hadoop/conf/masters").getLines.toList.head

  def parseTable(input_prefix: String): Unit = {
    val conf = new SparkConf().setAppName("Convert tbl to parquet")
    // read from alluxio
    val INPUT_DIR = s"${input_prefix}/data"

    val OUTPUT_DIR = s"alluxio://${IP}:19998/home/ec2-user/tpch_parquet"
    
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    sqlContext.setConf("spark.sql.parquet.compression.codec", "uncompressed")

    val schemaProvider = new TpchSchemaProvider(sc, INPUT_DIR)

    import sqlContext.implicits._
    import schemaProvider._
    schemaProvider.customer.write.format("parquet").save(s"${OUTPUT_DIR}/customer.parquet")
    schemaProvider.lineitem.write.format("parquet").save(s"${OUTPUT_DIR}/lineitem.parquet")
    schemaProvider.nation.write.format("parquet").save(s"${OUTPUT_DIR}/nation.parquet")
    schemaProvider.region.write.format("parquet").save(s"${OUTPUT_DIR}/region.parquet")
    schemaProvider.order.write.format("parquet").save(s"${OUTPUT_DIR}/orders.parquet")
    schemaProvider.part.write.format("parquet").save(s"${OUTPUT_DIR}/part.parquet")
    schemaProvider.partsupp.write.format("parquet").save(s"${OUTPUT_DIR}/partsupp.parquet")
    schemaProvider.supplier.write.format("parquet").save(s"${OUTPUT_DIR}/supplier.parquet")

  }
}