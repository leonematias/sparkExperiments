package com.leonematias.common

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object Utils {



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

}
