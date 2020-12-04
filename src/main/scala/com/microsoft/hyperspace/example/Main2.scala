package com.microsoft.hyperspace.example
// scalastyle:off

import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, InMemoryFileIndex, LogicalRelation}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import com.microsoft.hyperspace.Hyperspace
import com.microsoft.hyperspace.index.IndexConfig
import com.microsoft.hyperspace.Implicits
import com.microsoft.hyperspace.Hyperspace
import com.microsoft.hyperspace.index.IndexConfig

object Main2 extends SparkApp {
  val path = "glob2/*"
  val basePath = "glob2"
  spark.read.option("header", "true").option("basePath", basePath).csv(path).show()

  spark.read.option("header", "true").csv(path).show()

  spark.read.option("header", "true").csv(basePath) //.show()

  val basePath2 = "glob"
  val path2 = "glob/*/1"
  var df = spark.read.option("header", "true").option("basePath", basePath2).csv(path2)
  df.show()

  val locs = df.queryExecution.analyzed.collect {
    case LogicalRelation(HadoopFsRelation(location: InMemoryFileIndex, _, _, _, _, _), _, _, _) =>
      location.rootPaths
  }.flatten
  locs.foreach(println)
}

object CreateIndex2 extends SparkApp {
  val path = "glob2"
  val df = spark.read.option("header", "true").csv(path)
  val hs = new Hyperspace(spark)
  hs.createIndex(df, IndexConfig("i", Seq("c1"), Seq("y")))
}

object RefreshIndex2 extends SparkApp {
  val hs = new Hyperspace(spark)
  hs.refreshIndex2("i", "incremental")
}

object ShowIndex extends SparkApp {
  spark.read
    .parquet("C:\\Users\\apdave\\github\\hyperspace-1\\spark-warehouse\\indexes\\i")
    .show()
}
object FilterRule2 extends SparkApp {
  val path = "glob2"
  val schema = StructType(
    Seq(
      StructField("c1", IntegerType),
      StructField("c1", StringType),
      StructField("y", StringType)))
  val df = spark.read
    .option("header", "true")
    .csv(path)

  val hs = new Hyperspace(spark)
  spark.enableHyperspace()

  val df2 = df.filter("c1 > 20").select("c1", "y")
  df2.explain(true)
  println(df2.queryExecution.optimizedPlan)
  df2.show()
  hs.explain(df2)
  println(df2.queryExecution.optimizedPlan)
}
