# tpch-spark

TPC-H queries implemented in Spark using the DataFrames API.
Tested under Spark 2.0.0

Savvas Savvides

savvas@purdue.edu

### Running

First compile using:

```bash
sbt assembly
```

Make sure you set the INPUT_DIR and OUTPUT_DIR in TpchQuery class before compiling to point to the
location the of the input data and where the output should be saved.

You can then run a query on plain tbl using:

```bash
spark-submit --class "main.scala.TpchQuery" --master MASTER target/scala-2.11/spark-tpc-h-queries_2.11-1.0.jar --query ##

# In our project, eg.
/home/ec2-user/spark/bin/spark-submit  --class "main.scala.TpchQuery" --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 /home/ec2-user/tpch-spark/target/scala-2.11/spark-tpc-h-queries_2.11-1.0.jar --query 2
```

where ## is the number of the query to run e.g 1, 2, ..., 22
and MASTER specifies the spark-mode e.g local, yarn, standalone etc...

You can convert tables to parquet by:

```bash
spark-submit --class "main.scala.TpchQuery" --master MASTER target/scala-2.11/spark-tpc-h-queries_2.11-1.0.jar --convert-table

# In our project, eg.
/home/ec2-user/spark/bin/spark-submit  --class "main.scala.TpchQuery" --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 /home/ec2-user/tpch-spark/target/scala-2.11/spark-tpc-h-queries_2.11-1.0.jar --convert-table
```

You can run queries on  parquet by:

```bash
spark-submit --class "main.scala.TpchQuery" --master MASTER target/scala-2.11/spark-tpc-h-queries_2.11-1.0.jar --query ## --run-parquet

# In our project, eg.
/home/ec2-user/spark/bin/spark-submit  --class "main.scala.TpchQuery" --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 /home/ec2-user/tpch-spark/target/scala-2.11/spark-tpc-h-queries_2.11-1.0.jar --query 2 --run-parquet

```
### Other Implementations

1. Data generator (http://www.tpc.org/tpch/)

2. TPC-H for Hive (https://issues.apache.org/jira/browse/hive-600)

3. TPC-H for PIG (https://github.com/ssavvides/tpch-pig)
