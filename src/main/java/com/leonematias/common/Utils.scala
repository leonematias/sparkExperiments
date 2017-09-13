package com.leonematias.common

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}


object Utils {


  /**
    * Init spark cluster
    */
  def initSpark(): SparkSession = {
    val sparkConf = new SparkConf()
      .setAppName("Spark Experiments")
      .setMaster("local[2]")

    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    spark
  }

  def loadCsvDataFrame(spark: SparkSession, filePath: String, sep: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("delimiter", sep)
      .option("inferSchema", "true")
      .csv(filePath)
  }

}
