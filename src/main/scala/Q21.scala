package main.scala

import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.udf

/**
 * TPC-H Query 21
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q21 extends TpchQuery {

  override def execute(sc: SparkContext, schemaProvider: TpchSchemaProvider): DataFrame = {

    // this is used to implicitly convert an RDD to a DataFrame.
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    import schemaProvider._

    val fsupplier = supplier.select($"s_suppkey", $"s_nationkey", $"s_name")

    val plineitem = lineitem.select($"l_suppkey", $"l_orderkey", $"l_receiptdate", $"l_commitdate")
    //cache

    val flineitem = plineitem.filter($"l_receiptdate" > $"l_commitdate")
    // cache

    val line1 = plineitem.groupBy($"l_orderkey")
      .agg(countDistinct($"l_suppkey").as("suppkey_count"), max($"l_suppkey").as("suppkey_max"))
      .select($"l_orderkey".as("key"), $"suppkey_count", $"suppkey_max")

    val line2 = flineitem.groupBy($"l_orderkey")
      .agg(countDistinct($"l_suppkey").as("suppkey_count"), max($"l_suppkey").as("suppkey_max"))
      .select($"l_orderkey".as("key"), $"suppkey_count", $"suppkey_max")

    val forder = order.select($"o_orderkey", $"o_orderstatus")
      .filter($"o_orderstatus" === "F")

    nation.filter($"n_name" === "SAUDI ARABIA")
      .join(fsupplier, $"n_nationkey" === fsupplier("s_nationkey"))
      .join(flineitem, $"s_suppkey" === flineitem("l_suppkey"))
      .join(forder, $"l_orderkey" === forder("o_orderkey"))
      .join(line1, $"l_orderkey" === line1("key"))
      .filter($"suppkey_count" > 1 || ($"suppkey_count" == 1 && $"l_suppkey" == $"max_suppkey"))
      .select($"s_name", $"l_orderkey", $"l_suppkey")
      .join(line2, $"l_orderkey" === line2("key"), "left_outer")
      .select($"s_name", $"l_orderkey", $"l_suppkey", $"suppkey_count", $"suppkey_max")
      .filter($"suppkey_count" === 1 && $"l_suppkey" === $"suppkey_max")
      .groupBy($"s_name")
      .agg(count($"l_suppkey").as("numwait"))
      .sort($"numwait".desc, $"s_name")
      .limit(100)
  }
  override def getRawSQL(): String = {
    return "select s_name, count(*) as numwait from SUPPLIER, LINEITEM l1, ORDERS, NATION where s_suppkey = l1.l_suppkey and o_orderkey = l1.l_orderkey and o_orderstatus = 'F' and l1.l_receiptdate > l1.l_commitdate and exists ( select * from LINEITEM l2 where l2.l_orderkey = l1.l_orderkey and l2.l_suppkey <> l1.l_suppkey) and not exists (select * from LINEITEM l3 where l3.l_orderkey = l1.l_orderkey and l3.l_suppkey <> l1.l_suppkey and l3.l_receiptdate > l3.l_commitdate) and s_nationkey = n_nationkey and n_name = 'EGYPT' group by s_name order by numwait desc, s_name limit 100"
  }

}
