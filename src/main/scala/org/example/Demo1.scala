package org.example

import java.nio.file.{Files, Paths}

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro._
import org.apache.spark.sql.functions._
import org.example.Utils._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
//import com.google.cloud.spark._

object Demo1 {
  def main(args: Array[String]): Unit = {


    val conf: SparkConf = new SparkConf()
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    println("file",readFileFromResource.readFromResource("/schema/sam.avsc").getAbsolutePath)

    val spark = SparkSession.builder().master("local").appName("WriteToBigquery").config(conf).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")


      val df = spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9094")
        .option("subscribe", "demo")
        .option("startingOffsets", "latest").load()

      val jsonFormatSchema = new String(
        Files.readAllBytes(Paths.get(readFileFromResource.readFromResource("/schema/sam.avsc").getAbsolutePath)))

      val personDF = df.select(from_avro(col("value"), jsonFormatSchema).as("person"))
        .select("person.*")

     /* personDF.writeStream.outputMode("append").format("console")
      .option("checkpointLocation", "/home/slave/df4")
      .start()
      .awaitTermination() */

        personDF.writeStream.outputMode("append").format("csv")
        .option("checkpointLocation", "gs://shuvabucket/")
        .start( "gs://mycsv2/demo1")
        .awaitTermination()

   /* personDF.writeStream.option("checkpointLocation", "gs://shuvabucket")
      .option("table","serene-radius-275116.mydata.mycsv")
      .format("bigquery")
      .start()
      .awaitTermination() */

    }



}