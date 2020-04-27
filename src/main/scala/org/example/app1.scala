package org.example

import java.nio.file.{Files, Paths}
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, DataFrame, Dataset, ForeachWriter, Row, SparkSession}
import org.apache.spark.sql.avro._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

import scala.concurrent.Future


object app1 {

  val conf: SparkConf = new SparkConf()
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  conf.setMaster("local[*]")

  val spark = SparkSession.builder().appName("WriteToES").config(conf).getOrCreate()
  import spark.implicits._
  spark.sparkContext.setLogLevel("ERROR")


  def demo() ={

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "demo")
      .option("startingOffsets", "latest") // From starting
      .load()

    val jsonFormatSchema = new String(
      Files.readAllBytes(Paths.get("/home/slave/IdeaProjects/Practice/src/main/resources/schema/sam.avsc")))

    val personDF = df.select(from_avro(col("value"), jsonFormatSchema).as("person"))
      .select("person.*")

     personDF.writeStream.outputMode("append").format("console")
       .option("checkpointLocation", "/home/slave/kafka/logs/df")
       .start()
      // .awaitTermination()

  }

  def demo1() ={

    val df1 = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "demo1")
      .option("startingOffsets", "latest") // From starting
      .load()

    val Schema = new String(
      Files.readAllBytes(Paths.get("/home/slave/IdeaProjects/Practice/src/main/resources/schema/sam1.avsc")))

    val personDF1 = df1.select(from_avro(col("value"), Schema).as("person1"))
      .select("person1.*")

    personDF1.writeStream.outputMode("append").format("console")
      .option("checkpointLocation", "/home/slave/kafka/logs/df1")
      .start()
      //.awaitTermination()

  }


  def main(args: Array[String]): Unit = {

    while(true) {
      val f1 = Future {
        demo()
      } //work starts here
      val f2 = Future {
        demo1()
      } //and here

      val result = for {
        r1 <- f1
        r2 <- f2

      } yield (r1, r2)



      // important for a little parallel demo: keep the jvm alive
      sleep(30)

      def sleep(time: Long): Unit = Thread.sleep(time)
    }
  }


}
