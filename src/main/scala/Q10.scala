package main.scala

import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf

/**
 * TPC-H Query 10
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q10 extends TpchQuery {

  override def execute(sc: SparkContext, schemaProvider: TpchSchemaProvider): DataFrame = {

    // this is used to implicitly convert an RDD to a DataFrame.
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    import schemaProvider._

    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }

    val flineitem = lineitem.filter($"l_returnflag" === "R")

    order.filter($"o_orderdate" < "1994-01-01" && $"o_orderdate" >= "1993-10-01")
      .join(customer, $"o_custkey" === customer("c_custkey"))
      .join(nation, $"c_nationkey" === nation("n_nationkey"))
      .join(flineitem, $"o_orderkey" === flineitem("l_orderkey"))
      .select($"c_custkey", $"c_name",
        decrease($"l_extendedprice", $"l_discount").as("volume"),
        $"c_acctbal", $"n_name", $"c_address", $"c_phone", $"c_comment")
      .groupBy($"c_custkey", $"c_name", $"c_acctbal", $"c_phone", $"n_name", $"c_address", $"c_comment")
      .agg(sum($"volume").as("revenue"))
      .sort($"revenue".desc)
      .limit(20)
  }

  override def getRawSQL(): String = {
    return "select c_custkey,\n\tc_name,\n\tsum(l_extendedprice * (1 - l_discount)) as revenue,\n\tc_acctbal,\n\tn_name,\n\tc_address,\n\tc_phone,\n\tc_comment\nfrom\n\tCUSTOMER,\n\tORDERS,\n\tLINEITEM,\n\tNATION\nwhere\n\tc_custkey = o_custkey\n\tand l_orderkey = o_orderkey\n\tand o_orderdate >= date '1993-08-01'\n\tand o_orderdate < date '1993-08-01' + interval '3' month\n\tand l_returnflag = 'R'\n\tand c_nationkey = n_nationkey\ngroup by\n\tc_custkey,\n\tc_name,\n\tc_acctbal,\n\tc_phone,\n\tn_name,\n\tc_address,\n\tc_comment\norder by\n\trevenue desc\nlimit 20"
  }

}
