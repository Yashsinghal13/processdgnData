package com.octro.start.main

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object Testing3 {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("saveTextFile").master("local[*]").getOrCreate()
    // Read the text file as RDD
    val inputRDD = spark.sparkContext.textFile("/Users/yashsinghal/Downloads/ProcessDGNData/src/main/sample.txt")
    inputRDD.saveAsTextFile("/Users/yashsinghal/Downloads/ProcessDGNData/src/main/scala/dgn/lts/kafka_events_new/2025-03-01")
  }
}
