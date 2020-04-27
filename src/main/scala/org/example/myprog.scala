package org.example

import java.nio.file.{Files, Paths}

import com.google.cloud.spark.bigquery._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro._
import org.apache.spark.sql.functions.col
import java.sql.Timestamp


import org.example.Utils.readFileFromResource


object myprog {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf()
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.setMaster("local[*]")

    val spark = SparkSession.builder().appName("WriteToES").config(conf).config("spark.es.nodes.discovery", "false").config("spark.es.cluster.name", "kibana-cluster").config("spark.es.port", "9200").config("es.nodes.wan.only", "true").getOrCreate()
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
      .option("path", "gs://mytab1/mytopic")
      .start().awaitTermination()

  }
}

