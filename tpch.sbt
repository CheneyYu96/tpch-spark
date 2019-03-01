name := "Spark TPC-H Queries"

version := "1.0"

scalaVersion := "2.11.12"

assemblyJarName in assembly := "spark-tpc-h-queries_2.11-1.0.jar"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0" % "provided"
libraryDependencies += "com.github.scopt" %% "scopt" % "3.7.1"