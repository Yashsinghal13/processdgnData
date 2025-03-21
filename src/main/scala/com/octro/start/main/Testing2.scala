package com.octro.start.main

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

object Testing2 {
  def main(args: Array[String]): Unit = {
    val end_time = DateTime.now(Format.utcZone)
    val start_time = end_time.minusHours(600)
    var current_time = start_time
    val dateMap: mutable.Map[String, List[List[(String, Boolean)]]] = mutable.Map() //"2025/03/01 ----> [[(path1,false),(path2,false),(path3,false)],[(path1,false),(path2,false),(path3,false)],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[]]
    while (current_time.isBefore(end_time) || current_time.isEqual(end_time)) {
      val hourpath = current_time.toString(Format.hourFmt) //2025/03/01/00
      OPrinter.logAndPrint(current_time.toString(Format.hourFmt))
      current_time = current_time.plusHours(1)
      val finalPath = "/dgn/ovs/raw_events/ovs/events/prod/" + hourpath
      val pathList = listHdfsFilesWithModificationTime(finalPath)
      //OPrinter.logAndPrint(pathList.size.toString)
      val filteredPathList = pathList
        .filter { case (_, timestamp) =>
          val filetimestamp = DateTime.parse(timestamp, Format.fmt)
          val current = DateTime.now(Format.utcZone)
          Minutes.minutesBetween(filetimestamp, current).getMinutes > 5
        }
      val date = hourpath.take(10) // "2025/03/01"
      val hour = hourpath.takeRight(2).toInt // "00"
      if (!dateMap.contains(date)) {
        dateMap(date) = List.fill(24)(List.empty[(String, Boolean)]) // Initialize with 24 empty lists
      }
      // Update the inner list
      var updatedInnerList = dateMap(date)(hour)
      filteredPathList.foreach { case (filePath, _) =>
        val cleanedPath = filePath.substring(filePath.indexOf("/dgn"))
        updatedInnerList = updatedInnerList :+ (cleanedPath, false)
      }
      dateMap(date) = dateMap(date).updated(hour, updatedInnerList)


      //need to remove after the testing
      if (!dateMap.contains("2025/03/01")) {
        dateMap("2025/03/01") = List.fill(24)(List.empty[(String, Boolean)]) // Initialize with 24 empty lists
      }
      val updateInnerlist = List(("123", false), ("456", false), ("789", false), ("567", false))
      dateMap("2025/03/01") = dateMap("2025/03/01").updated(0, updateInnerlist)
      //println(dateMap)
      filteringAlreadyProcessedPath(dateMap)
      println(dateMap)
      hourwiseDataReader(dateMap)
    }
  }
  def filteringAlreadyProcessedPath(dateMap: mutable.Map[String, List[List[(String, Boolean)]]]): Unit = {
    val connection = HbaseConnectionUtilProd.getConnection()
    val tableName = "already_processed_paths"
    val table = connection.getTable(TableName.valueOf(tableName))
    println("Read data from successfully with hbase")
    val hbaseData: mutable.Map[String, List[String]] = mutable.Map()
    dateMap.foreach {
      case (date, fileList) =>
        val columnFamily = "hour_data"
        val get = new Get(Bytes.toBytes(date))
        val result: Result = table.get(get)
        if(!result.isEmpty) {
          for (i <- 0 to 23) {
            val hour = f"$i%02d"
            val value = result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(hour))
            if(value!=null)
            {
                val processedPaths=Bytes.toString(value).split(',').toSet
                println(processedPaths)
                val updatedFileList = dateMap(date)(i).map {
                case (path, existence) => (path, processedPaths.contains(path))
              }
              dateMap(date) = dateMap(date).updated(i,updatedFileList)
            }
          }
        }
    }
    println("Hourmap updated Successfully")
  }
  def listHdfsFilesWithModificationTime(directoryPath: String): List[(String, String)] = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val path = new Path(directoryPath)
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") // Format timestamp
    if (fs.exists(path) && fs.isDirectory(path)) {
      val fileStatuses = fs.listStatus(path)
      fileStatuses.filter(_.isFile).map(status => {
        val filePath = status.getPath.toString
        val modificationTime = dateFormat.format(new Date(status.getModificationTime))
        (filePath, modificationTime)
      }).toList
    } else {
      OPrinter.logAndPrint(s"Path $directoryPath does not exist or is not a directory")
      List.empty
    }
  }
  def hourwiseDataReader(dateMap: mutable.Map[String, List[List[(String, Boolean)]]]): Unit =
  {
    val connection = HbaseConnectionUtilProd.getConnection()
    val tableName = "already_processed_paths"
    val table = connection.getTable(TableName.valueOf(tableName))
    println("Connection made Successfully for write");
    val spark=SparkSession.builder().appName("dedx").master("local[*]").getOrCreate()
    //val accum = spark.sparkContext.longAccumulator("api_hits")
    try {
      val dateHourWisePaths: Map[(String, Int), List[String]] = dateMap.flatMap { case (date, hourlyLists) =>
        hourlyLists.zipWithIndex.flatMap { case (hourlyList, hour) =>
          val falsePaths: List[String] = hourlyList.asInstanceOf[List[(String, Boolean)]].collect { case (path: String, flag: Boolean) if !flag => path }
          if (falsePaths.nonEmpty) Some((date, hour) -> falsePaths) else None
        }
      }.toMap
      println(dateHourWisePaths);
      dateHourWisePaths.foreach { case ((date, hour), paths) =>
        val formattedHour = f"$hour%02d" // Ensure "00", "01", etc.
        println(s"Reading data for Date: $date, Hour: $formattedHour, Paths: $paths")
        val events = spark.read.option("lineSep", "**##**").text(paths: _*)
        events.show(false)
      }
    }
  }
}
