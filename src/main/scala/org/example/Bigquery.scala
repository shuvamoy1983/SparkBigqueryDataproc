package org.example

import java.nio.file.{Files, Paths}

import com.google.cloud.spark.bigquery._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro._
import org.apache.spark.sql.functions.col
import java.sql.Timestamp
import scala.concurrent.ExecutionContext.Implicits.global
import org.example.Utils.readFileFromResource

import scala.concurrent.Future


object Bigquery {

  val conf: SparkConf = new SparkConf()
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  conf.setMaster("local[*]")

  val spark = SparkSession.builder().appName("WriteToES").config(conf).config("spark.es.nodes.discovery", "false").config("spark.es.cluster.name", "kibana-cluster").config("spark.es.port", "9200").config("es.nodes.wan.only", "true").getOrCreate()

  def firstjob() = {
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("temporaryGcsBucket", "shuvabucket")
    spark.conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    spark.conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")


    //content of this dataframe will be in binary format
    val emptopic = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "35.184.202.107:9094")
      .option("subscribe", "demo")
      .option("startingOffsets", "latest") //latest
      .load()

    val jsonFormatSchema = new String(
      Files.readAllBytes(Paths.get(readFileFromResource.readFromResource("/schema/sam.avsc").getAbsolutePath)))

    val ftopic = emptopic.selectExpr("CAST(value AS BINARY)", "CAST(topic AS STRING)", "CAST(offset AS LONG)", "CAST(timestamp AS TIMESTAMP)")
      .as[(String, String, String, Timestamp)]
      .select(from_avro($"value", jsonFormatSchema).as("data"))
      .select("data.*")

    ftopic.writeStream
      .format("csv")
      .outputMode("append")
      .option("checkpointLocation", "gs://shuvabucket/")
      .option("path", "gs://mytab1/firstopic")
      .start()
  }

  def secondjob() = {
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("temporaryGcsBucket", "shuvabucket")
    spark.conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    spark.conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

    //content of this dataframe will be in binary format
    val datatopic = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "35.184.202.107:9094")
      .option("subscribe", "demo1")
      .option("startingOffsets", "latest") //latest
      .load()

    val avroSchema = new String(
      Files.readAllBytes(Paths.get(readFileFromResource.readFromResource("/schema/sam1.avsc").getAbsolutePath)))

    val dtopic = datatopic.selectExpr("CAST(value AS BINARY)", "CAST(topic AS STRING)", "CAST(offset AS LONG)", "CAST(timestamp AS TIMESTAMP)")
      .as[(String, String, String, Timestamp)]
      .select(from_avro($"value", avroSchema).as("data"))
      .select("data.*")

    dtopic.writeStream
      .format("csv")
      .outputMode("append")
      .option("checkpointLocation", "gs://shuvabucket/df2")
      .option("path", "gs://mycsv2/secondtopic")
      .start()
  }

  def main(args: Array[String]): Unit = {
    while (true) {
      val f1 = Future {
        firstjob()
      }

      val f2 = Future {
        secondjob()
      } //and here

      val result = for {
        r1 <- f1
        r2 <- f2

      } yield (r1, r2)

      // important for a little parallel demo: keep the jvm alive
      sleep(10)

      def sleep(time: Long): Unit = Thread.sleep(time)

    }
  }
}
