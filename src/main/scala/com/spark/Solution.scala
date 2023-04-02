package com.spark

import com.datastax.spark.connector._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}


object Solution {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CsvToKafkaExample")
      .master("local[3]")
      .config("spark.cassandra.connection.host","localhost")
      .config("spark.cassandra.connection.port","9042")
      .getOrCreate()


    val schema = StructType(List(
      StructField("NPI", StringType),
      StructField("ProviderSecondaryPracticeLocationAddressAddressLine1", StringType),
      StructField("ProviderSecondaryPracticeLocationAddressAddressLine2", StringType),
      StructField("ProviderSecondaryPracticeLocationAddressCityName", StringType),
      StructField("ProviderSecondaryPracticeLocationAddressStateName", StringType),
      StructField("ProviderSecondaryPracticeLocationAddressPostalCode", StringType),
      StructField("ProviderSecondaryPracticeLocationAddressCountryCode", StringType),
      StructField("ProviderSecondaryPracticeLocationAddressTelephoneNumber", StringType),
      StructField("ProviderSecondaryPracticeLocationAddressTelephoneExtension", StringType),
      StructField("ProviderPracticeLocationAddressFaxNumber", StringType),
    ))

    val schema1 = StructType(List(
      StructField("NPI", StringType),
      StructField("NPPESDeactivationDate", StringType),
    ))

    val weeklyDF = spark.read
      .option("header", "true")
      .schema(schema)
      .csv("WeeklyData/pl_pfile_20230306-20230312.csv")

    val monthlyDF = spark.read
      .option("header", "true")
      .schema(schema1)
      .csv("monthlyData/NPPESDeactivatedNPIReport20230313.csv")


//   val weekly = weeklyDF.withColumnRenamed("NPI","id")

   val joinedDF = weeklyDF.join(monthlyDF, Seq("NPI"), "left_anti")
    val joined_DF = weeklyDF.join(monthlyDF, Seq("NPI"), "inner").drop("NPPESDeactivationDate")

    // val joinedDF = weekly.join(monthlyDF,weekly("id") === monthlyDF("NPI"),"left_anti")
//    joinedDF.show(20,false)

    val joinedDFnew = joinedDF.withColumnRenamed("NPI","id")
    val joined_DFnew = joined_DF.withColumnRenamed("NPI","id")


 val kafkaDF = joinedDF.selectExpr("NPI as key", "to_csv(struct(*)) as value")


    kafkaDF.write
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "notifications")
      .save()


 val activate =  joinedDFnew.select(joinedDFnew.columns.map(x => col(x).as(x.toLowerCase)): _*)
 val deactivate =  joined_DFnew.select(joined_DFnew.columns.map(x => col(x).as(x.toLowerCase)): _*)




    activate.write
      .format("org.apache.spark.sql.cassandra")
      .option("table", "my_table")
      .option("keyspace", "my_keyspace")
      .mode("append")
      .save()


        deactivate.write
          .format("org.apache.spark.sql.cassandra")
          .option("table", "deactivate_file")
          .option("keyspace", "my_keyspace")
          .mode("append")
          .save()



  }
}


//for see output
// kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic notifications --from-beginning