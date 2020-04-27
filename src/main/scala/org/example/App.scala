package org.example

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro._
import org.apache.spark.sql.functions.struct

object App extends App {
  val conf: SparkConf = new SparkConf()
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  //conf.set("spark.driver.bindAddress", "127.0.0.1")
  conf.setMaster("local[*]")

  val spark = SparkSession.builder.appName("Read and write with schema").config(conf).getOrCreate()

  val sampleData = spark.read.format("csv").option("header", "true").option("inferSchema", "true")
    .load("gs://mysrcfile/sampledata.csv")
  sampleData.show(2)

  sampleData.select(to_avro(struct("*")) as "value").write.format("kafka")
    .option("kafka.bootstrap.servers", "35.184.202.107:9094")
    .option("kafka.request.required.acks", "1")
    .option("topic", "demo").save()

  val Data = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("gs://mysrcfile/data.csv")
  Data.show(2)

     Data.select(to_avro(struct("*")) as "value")
    .write
    .format("kafka")
    .option("kafka.bootstrap.servers", "35.184.202.107:9094")
    .option("kafka.request.required.acks", "1")
    .option("topic", "demo1")
    .save()


}
