package StructureStreaming

import java.time.{LocalDate, Period}


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructType}

/**
 * Hello world!
 *
  *  ./zookeeper-server-start /Users/shuvamoymondal/confluent/etc/kafka/zookeeper.properties
./kafka-server-start /Users/shuvamoymondal/confluent/etc/kafka/server.properties

bin/kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic con

./connect-standalone /Users/shuvamoymondal/confluent/etc/kafka/connect-standalone.properties /Users/shuvamoymondal/confluent/etc/kafka/connect-file-source.properties

/Users/shuvamoymondal/spark22/bin/spark-submit --class StructureStreaming.App  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.2  /Users/shuvamoymondal/Downloads/StructureStreaming/target/StructureStreaming-1.0-SNAPSHOT.jar

Sample data:
{"firstName":"Quentin","lastName":"Adam","birthDate":"1988-10-26T03:52:14.449+0000"}
 */


object App {
  def main(args: Array[String]): Unit = {

    new StreamsProcessor("localhost:9092").process()

  }
}

  class StreamsProcessor(brokers: String) {

    def process(): Unit = {

      val spark = SparkSession.builder()
        .appName("kafka-tutorials")
        .master("local[*]")
        .getOrCreate()

      import spark.implicits._

      val inputDf = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "con1")
        .option("startingoffsets", "latest")
        .load()

      val personJsonDf = inputDf.selectExpr("CAST(value AS STRING)")

      val struct = new StructType()
        .add("firstName", DataTypes.StringType)
        .add("lastName", DataTypes.StringType)
        .add("birthDate", DataTypes.StringType)
      val personNestedDf = personJsonDf.select(from_json($"value", struct).as("person"))


      val personFlattenedDf = personNestedDf.selectExpr("person.firstName", "person.lastName", "person.birthDate")

      val personDf = personFlattenedDf.withColumn("birthDate", to_timestamp($"birthDate", "yyyy-MM-dd'T'HH:mm:ss.SSSZ"))

      val ageFunc: java.sql.Timestamp => Int = birthDate => {
        val birthDateLocal = birthDate.toLocalDateTime().toLocalDate()
        val age = Period.between(birthDateLocal, LocalDate.now()).getYears()
        age
      }
      val ageUdf: UserDefinedFunction = udf(ageFunc, DataTypes.IntegerType)
      val processedDf = personDf.withColumn("age", ageUdf.apply($"birthDate"))

      val resDf = processedDf.select(
        concat($"firstName", lit(" "), $"lastName").as("key"),
        processedDf.col("age").cast(DataTypes.StringType).as("value"))



      val consoleOutput = resDf.writeStream
        .outputMode("append")
        .format("console")
        .start()


      consoleOutput.awaitTermination()
    }
  }