package main.scala

import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf

/**
  *
  */
class Q30 extends TpchQuery {

  override def execute(sc: SparkContext, schemaProvider: TpchSchemaProvider): DataFrame = {

    // this is used to implicitly convert an RDD to a DataFrame.
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._
    import schemaProvider._

    lineitem.join(order, $"l_orderkey" === order("o_orderkey")).limit(10)

  }
  override def getRawSQL(): String = {
    return "select l_orderkey, o_orderdate, o_comment, l_commitdate, l_shipdate, l_receiptdate, l_discount, l_comment from ORDERS, LINEITEM where l_orderkey = o_orderkey"
//    return "select * from ORDERS, LINEITEM where l_orderkey = o_orderkey"
  }
}