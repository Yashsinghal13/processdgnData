package com.octro.start.main

import com.octro.utilities.{HbaseConnectionUtilProd, OPrinter}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Get, Put}
import org.apache.hadoop.hbase.util.Bytes
import com.google.gson.{Gson, JsonObject, JsonParser, JsonPrimitive}
import com.typesafe.config.ConfigFactory
import java.util.Date
import org.apache.hadoop.conf.Configuration
import java.text.SimpleDateFormat
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode, SparkSession, functions}
import org.joda.time.{DateTime, DateTimeZone, Days,Minutes}
import scalaj.http.Http
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.util.Bytes
import com.octro.utilities.{Format, Notifier, OPrinter}
import com.octro.utilities.HbaseConnectionUtilProd
import com.octro.utilities.OKafkaProducer
import org.joda.time.format.DateTimeFormat
import scala.collection.mutable.ListBuffer
import java.io.File
import scala.collection.mutable
//import com.octro.utils.OKafkaProducer
import org.apache.commons.lang.exception.ExceptionUtils
import com.google.gson.JsonNull
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Append, Get, HTable, Increment, Put, Result, Scan, Table}
import java.security.MessageDigest
import scala.collection.mutable

object Testing {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("hdsd").master("local[*]").getOrCreate()
    val hourPath="2025/03/02"
    val hour = hourPath.takeRight(2)
    val date = hourPath.take(10)
    val tableName = "already_processed_paths"
    val columnFamily = "hour_data" // Now using hour ("00", "01", ..., "23") as column family
    //val columnQualifier = hour
    val hbaseData: mutable.Map[String, List[String]] = mutable.Map()
    val connection = HbaseConnectionUtilProd.getConnection()
    val table: Table = connection.getTable(TableName.valueOf(tableName))
    val get = new Get(Bytes.toBytes(date))
    val result: Result = table.get(get)
    var valuesList: List[String] = List.fill(23)("") // Default size 23, filled with empty strings

    // If row exists, fetch values
    val qualifiers = (0 to 23).map(i => f"$i%02d").toList
    if (!result.isEmpty) {
      valuesList = qualifiers.map { qualifier =>
        val value = result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier))
        if (value != null) Bytes.toString(value) else "" // Handle null values
      }
      hbaseData.update(date, valuesList)
    } else
    {
      println(s"No data found for row key: $date")
    }
    println(hbaseData)
    println(hbaseData(date)(2))

//    if (!result.isEmpty) {
//      println("Fetched Row: " + result)
//      val value = result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("col"))
//      if (value != null) {
//        println(s"Value for column cf:col: ${Bytes.toString(value)}")
//      } else {
//        println("Column value is null or does not exist.")
//      }
//    } else {
//      println(s"No data found for row key: $date")
//    }
//    val hourmap: mutable.Map[String, List[(String, Boolean)]] = mutable.Map(
//      "2025/03/01/00" -> List(("123", false),("456",false),("789",false),("489",true))
//    )
//    println(hourmap)
    //hourwiseDataReader(hourmap)
    //filteringAlreadyProcessedPath(hourmap)
    //alreadyProcessedPath(List("123","456"),"2025/03/01/00")
  }

//  def hourwiseDataReader(hourPathToRead: mutable.Map[String, List[(String, Boolean)]]): Unit = {
//    val connection = HbaseConnectionUtilProd.getConnection()
//    val tableName = "already_processed_paths"
//    val table = connection.getTable(TableName.valueOf(tableName))
//    println("Connection made Successfully for write");
//    try {
//      hourPathToRead.foreach {
//        case (datehour, fileList) =>
//          val date = datehour.take(10)
//          val hour = datehour.takeRight(2)
//          val filepaths = ListBuffer[String]()
//          val allPaths = ListBuffer[String]()
//          fileList.foreach { case (path, flag) =>
//            if (!flag) {
//              filepaths += path
//            }
//            allPaths += path
//          }
//          if (filepaths.nonEmpty) {
//            val put = new Put(Bytes.toBytes(date))
//            val columnFamily = "hour_data"
//            val columnQualifier = hour
//            val allPathsString = allPaths.mkString(",")
//            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnQualifier), Bytes.toBytes(allPathsString))
//            table.put(put)
//            OPrinter.logAndPrint("Data Write Successfully in the Hbase")
//          }
//      }
//    }
//    catch {
//      case e: Exception =>
//        OPrinter.logAndPrint("Spark Context Stopped")
//        OPrinter.logAndPrint(e.getMessage)
//    }
//    finally {
//      table.close()
//      connection.close()
//    }
//  }
  def filteringAlreadyProcessedPath(hourmap: mutable.Map[String, List[(String, Boolean)]]): Unit = {
    val connection = HbaseConnectionUtilProd.getConnection()
    val tableName = "already_processed_paths"
    val table = connection.getTable(TableName.valueOf(tableName))
    println("Read data from successfully with hbase")
    hourmap.foreach {
      case (datehour, fileList) =>
        val hour = datehour.takeRight(2)
        val date = datehour.take(10)
        val columnFamily = "hour_data"
        val columnQualifier = hour
        val get = new Get(Bytes.toBytes(date))
        get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnQualifier))
        val result = table.get(get)
        val processedPaths = if (!result.isEmpty) {
          Bytes.toString(result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(columnQualifier))).split(",").toSet
        } else {
          Set.empty[String]
        }
        println(processedPaths)

        val updatedFileList = hourmap(datehour).map {
          case (path, existence) => (path, processedPaths.contains(path))
//          println(path, processedPaths.contains(path))
        }
        hourmap.update(datehour, updatedFileList)
        println(hourmap)
    }
    println("Hour MAP updated Successfully")
  }

}
