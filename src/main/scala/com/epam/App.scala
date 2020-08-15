package com.epam

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

object App {

  def func_to_test(int1: Int, int2: Int): Unit ={
      int1 * int2
  }

  def main(args : Array[String]) {
    println( "Hello World!" )

      System.setProperty("hadoop.home.dir", "/opt/spark/Spark")
      // Create Spark Session
      val sparkSession = SparkSession
          .builder
          .appName("airflow")
          .master("local[*]")
          .getOrCreate()

      import com.databricks.spark.avro._
      import sparkSession.implicits._
      import org.apache.spark.sql.functions._

      // Read expedia data from HDFS
      val expediaPath = "/home/elizabeth/data/input/"
      val expedia = sparkSession.read
          .format("com.databricks.spark.avro")
          .load(expediaPath)
      expedia.show

      val expediaNew = expedia
          .select("*")
          .filter($"id".isNotNull)
          .filter($"srch_adults_cnt">2)
      expediaNew.show

      // Write partitioned data to HDFS
      val resultPath = "/home/elizabeth/data/output/"
      val fs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
      val folderExists = fs.exists(new Path(resultPath))

      if (!folderExists) {
          expediaNew.withColumn("year", (year($"srch_ci"))).write.partitionBy("year")
              .avro(resultPath)
      }
  }

}
