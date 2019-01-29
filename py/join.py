from pyspark import SparkConf, SparkContext
# from pyspark.sql import HiveContext
from pyspark.sql import SparkSession
from pyspark.sql import Row

def join_two_tables():
	spark = SparkSession.builder.appName("Join two tables").getOrCreate()
	sc = spark.sparkContext

	# read data
	input_dir = 'alluxio://{}:19998/tpch'.format(get_master())

	order_df = spark.createDataFrame(convert_orders(sc.textFile('{}/orders.tbl'.format(input_dir))))
	item_df = spark.createDataFrame(convert_lineitem(sc.textFile('{}/lineitem.tbl'.format(input_dir))))

	# create views
	order_df.createOrReplaceTempView('orders')
	item_df.createOrReplaceTempView('lineitem')

	# query
	result_df = spark.sql(
		''' 
		select * from ORDERS, LINEITEM where l_orderkey = o_orderkey
		'''
		)

	result_df.rdd

def get_master():
	with open('/home/ec2-user/hadoop/conf/masters', 'r') as f:
		return f.readline().rstrip()


splitter = lambda l: l.split('|')
convert_orders = lambda rdd: rdd.map(splitter).map(lambda p: Row(o_orderkey=p[0], o_custkey=p[1], o_orderstatus=p[2], o_totalprice=p[3], \
	o_orderdate=p[4], o_orderpriority=p[5], o_clerk=p[6], o_shippriority=p[7], o_comment=p[8]))
convert_lineitem = lambda rdd: rdd.map(splitter).map(lambda p: Row(l_orderkey=p[0], l_partkey=p[1], l_suppkey=p[2], l_linenumber=p[3], \
	l_quantity=p[4], l_extendedprice=p[5], l_discount=p[6], l_tax=p[7], l_returnflag=p[8], l_linestatus=p[9], l_shipdate=p[10], \
	l_commitdate=p[11], l_receiptdate=p[12], l_shipinstruct=p[13], l_shipmode=p[14], l_comment=p[15]))


if __name__ == '__main__':
	join_two_tables()