package com.octro.start.main

import com.google.gson.{Gson, JsonObject, JsonParser, JsonPrimitive}
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode, SparkSession, functions}
import org.joda.time.{DateTime, DateTimeZone, Days, Hours}
import scalaj.http.Http
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.util.Bytes
import com.octro.utilities.{Format, Notifier, OPrinter}
import com.octro.utilities.HbaseConnectionUtilProd
import com.octro.utilities.OKafkaProducer
import org.joda.time.format.DateTimeFormat

import scala.collection.mutable.ListBuffer
import java.text.SimpleDateFormat
import java.util.Date

//import com.octro.utils.OKafkaProducer
import org.apache.commons.lang.exception.ExceptionUtils
import com.google.gson.JsonNull
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Append, Get, HTable, Increment, Put, Result, Scan, Table}
import java.security.MessageDigest
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object TestEvents {
  val dgnLtsTopic = "dgn_lts_events" //"DGN-Events"
  val dgn_map_userid_gocid_tbl = "dgn_lts_userid_gocid"
  val dgn_map_gocid_userid_tbl = "dgn_lts_gocid_userid"

  def md5(s: String): String = {
    val digest = MessageDigest.getInstance("MD5")
    val hashBytes = digest.digest(s.getBytes)
    hashBytes.map("%02x".format(_)).mkString
  }

  def formatTime(modTime: Long): String = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm")
    dateFormat.format(new Date(modTime))
  }

  def getCurrentTimestamp: String = {
    val now = LocalDateTime.now
    val formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
    now.format(formatter)
  }

  // Extract the datehour part from the path
  def extractDateHour(path: String): String = {
    val pattern = ".*/prod/(\\d{4}/\\d{2}/\\d{2}/\\d{2}).*".r
    path match {
      case pattern(datehour) => datehour
      case _ => ""
    }
  }

  def main(args: Array[String]): Unit = {
    OPrinter.logAndPrint("*************** Welcome to DGN-LTS Connector On 14th Jun'24  ***************")


    val conf = new SparkConf()
      .set("spark.streaming.backpressure.enabled", "true")
      .set("spark.streaming.kafka.maxRatePerPartition", "50") //100000 //50000 //25000  *60 (duration)
      .set("com.couchbase.nodes", "serv98.octro.net:8091") //For CouchBase
      .set("spark.ui.port", "4545")
      .set("spark.executor.memoryOverhead", "100m")
      .set("spark.hadoop.fs.defaultFS", "hdfs://octropc")

    val sc: SparkContext = new SparkContext(conf)
    val spark = new SQLContext(sc).sparkSession
    println("Application id=" + sc.applicationId)

    var numPartitions = 10
    try {
      val numExecutors = sc.getConf.getOption("spark.executor.instances").get
      val numCores = sc.getConf.getOption("spark.executor.cores").get
      numPartitions = numExecutors.toInt * numCores.toInt
    }
    catch {
      case e: Exception => {
        e.printStackTrace()
        numPartitions = 10
      }
    }

    //    val hourPaths = List[String]("19", "20", "21", "22", "23")
    //    val hourPaths = List[String]("00","01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23")
    val eventNames = List("transaction", "level_up", "install", "start_session", "facebook_connect","spin")
    //List("transaction", "level_up", "install", "start_session", "worlds", "spin", "facebook_connect")
    val transnCol = List("purchase_count", "price", "purchase_amount", "session_id", "event_id","purchase_origin","local_price","real_exchange_rate")
    val levelUpCol = List("session_id", "level", "vip_level", "event_id")
    val installCol = List("session_id", "event_id")
    val startSesCol = List("version", "balance", "platform_id", "session_id", "event_id", "level", "vip_level", "spins_per_spinner", "country_code", "avg_bet", "total_bet", "device_type")
    val facebookConnectCol = List("session_id", "fb_id", "event_id")
    val spinCol = List("bet_amount", "event_id", "session_id")

    var mapperEventCol: Map[String, List[String]] = Map("transaction" -> transnCol, "level_up" -> levelUpCol, "install" -> installCol, "start_session" -> startSesCol, "facebook_connect" -> facebookConnectCol, "spin" -> spinCol)

    var colList: Map[String, List[String]] = Map()

    for (event <- eventNames) {
      if (event.equalsIgnoreCase("transaction")) {
        colList = colList + (event -> transnCol)
      }
      if (event.equalsIgnoreCase("level_up")) {
        colList = colList + (event -> levelUpCol)
      }
      if (event.equalsIgnoreCase("install")) {
        colList = colList + (event -> installCol)
      }
      if (event.equalsIgnoreCase("start_session")) {
        colList = colList + (event -> startSesCol)
      }
      if (event.equalsIgnoreCase("facebook_connect")) {
        colList = colList + (event -> facebookConnectCol)
      }
      if (event.equalsIgnoreCase("spin")) {
        colList = colList + (event -> spinCol)
      }
    }

    try {


      val formatter = DateTimeFormat.forPattern("yyyy/MM/dd")
      val hourFmt = DateTimeFormat.forPattern("yyyy/MM/dd/HH");

//      val lastHourRead = readLastPathOnHDFS("/dgn/lts/schema_checkpoints", spark).trim //.first().getString(0)
//      println(s"lastHourRead is $lastHourRead  ${lastHourRead.getClass.getSimpleName}")
//
//      val last24HrDay1 = DateTime.parse(lastHourRead,hourFmt).minusHours(24).toString(hourFmt)
//      println(s"last24HrDay1 $last24HrDay1 ${last24HrDay1.getClass.getSimpleName}")

//      println(DateTime.now.toString(hourFmt))

      val currDay = DateTime.now.toString(hourFmt)
      println(s"$currDay")
      println(s"currDay $currDay ${currDay.getClass.getSimpleName} ${DateTime.now.toString(hourFmt)}")
      val last24HrDay2 = DateTime.parse(currDay,hourFmt).minusHours(24).toString(hourFmt)
      println(s"last24HrDay2 $last24HrDay2 ${last24HrDay2.getClass.getSimpleName}")


      val hadoopConf = new org.apache.hadoop.conf.Configuration()
      hadoopConf.set("fs.defaultFS", "hdfs://octropc")
      val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
      // val formatter = DateTimeFormat.forPattern("yyyy/MM/dd")
      val toRead= currDay//.toString(hourFmt)
      println("toRead", toRead)
      // val fs = FileSystem.get(new Configuration())

      val savedfilePaths =ListBuffer[(String,String,String)]()

      val df_read= spark.read.parquet("/tmp/simran/processed_paths/*")
      val hashed_col = df_read.select(col("hashed_path"))
      import spark.implicits._
      val hashed_list = hashed_col.as[String].collect().toSet//.show(10,false)
      println(hashed_list.size)

      //      val startTime1 = DateTime.parse(last24HrDay1, hourFmt)
//      val endTime1 = DateTime.parse(lastHourRead, hourFmt)
//
//      // Generate list of hours between start and end times
//      val hoursList1 = (0 to Hours.hoursBetween(startTime1, endTime1).getHours).map { h =>
//        startTime1.plusHours(h).toString(hourFmt)
//      }.toList

//      println(hoursList1)
      // Print the list of hours
//      hoursList1.foreach(x=>{
//        try{
//          val currentList = hdfs.listStatus(new Path(s"/dgn/lts/raw_events/events/prod/$x"))
//          currentList.foreach(x=>{
//            savedfilePaths += ((x.getPath.toString,  formatTime(x.getModificationTime),  md5(x.getPath.toString)))
//          })
//        }
//        catch {
//          case e: Exception => {
//          }
//        }
//      })

      // println(savedfilePaths)

      val allfilePaths =ListBuffer[(String,String,String)]()

      val startTime2 = DateTime.parse(last24HrDay2, hourFmt)
      val endTime2 = DateTime.parse(currDay, hourFmt)

      // Generate list of hours between start and end times
      val hoursList2 = (0 to Hours.hoursBetween(startTime2, endTime2).getHours).map { h =>
        startTime2.plusHours(h).toString(hourFmt)
      }.toList

      println(hoursList2)
      // Print the list of hours
      hoursList2.foreach(x=>{
        try{
          val currentList = hdfs.listStatus(new Path(s"/dgn/lts/raw_events/events/prod/$x"))
          currentList.foreach(x=>{
            allfilePaths += ((x.getPath.toString,  formatTime(x.getModificationTime),  md5(x.getPath.toString)))
          })
        }
        catch {
          case e: Exception => {
          }
        }
      })

//      val list1Hashes = savedfilePaths.map(_._3).toSet


      // df.write.mode(SaveMode.Overwrite).parquet("/tmp/simran/processed_paths")

      // Filter list2 based on the third element
      val resultList = allfilePaths.filterNot { case (_, _, hash) => hashed_list.contains(hash) }

      // Print the result
      resultList.take(5).foreach(println)
      println(s"Count of resultList: ${resultList.size}")


      val groupedData = resultList.groupBy {
        case (path, _, _) => extractDateHour(path)
      }

      // Sort the grouped data by the datehour part in the path
      val sortedGroupedData = groupedData.toSeq.sortBy(_._1).map { case (_, listBuffer) =>
        val sortedListBuffer = listBuffer.sortBy {
          case (path, _, _) => extractDateHour(path)
        } // Sort each group by the datehour part in the path
        sortedListBuffer.map { case (path, timestamp, hash) => s"$path,$timestamp,$hash" }.toList
      }.toList

      // Print the size of the outer list
      println(s"Size of the outer list: ${sortedGroupedData.size}")

      // Print the size of each inner list
      sortedGroupedData.zipWithIndex.foreach { case (innerList, index) =>
        println(s"Size of inner list $index: ${innerList.size}")
      }


      val status = hourwiseDataReader(sortedGroupedData, spark, eventNames, mapperEventCol, numPartitions)

      OPrinter.logAndPrint(s"Data proccessed for " + status)

    }
    catch{
      case e: Exception => {
        OPrinter.logAndPrint("Spark Context Stopped")
        spark.sparkContext.stop()
        Notifier.sendEMail("dgn_lts@octro.com", s"Error occurred " + e.getMessage, "simran.maurya@octrotalk.com", "Action required : Couldn't process dgn-lts data")
        OPrinter.logAndPrint(e.getMessage)
      }
    }

  }

  def hourwiseDataReader(hourPathToRead: List[List[(String)]], spark: SparkSession, eventNames: List[String], mapperEventCol: Map[String, List[String]], numPartitions: Int): String = {
    import spark.implicits._

    if (hourPathToRead.size != 0) {
      try {

        hourPathToRead.foreach(hour => {

          val pathsList = hour.map { element =>
            val path = element.split(",")(0)
            val timestamp = element.split(",")(1)
            val hash = element.split(",")(2)
            (path,hash,timestamp)
          }

          val paths= pathsList.map(path =>path._1)

          val accum = spark.sparkContext.longAccumulator("api_hits")


          OPrinter.logAndPrint(s"Reading data for hour ${paths.head.split("prod/")(1).slice(0, 13)}")
          val jsonRDD = spark.sparkContext.textFile(paths.mkString(","))
            .flatMap(json => json.split("\\*##\\*")).map(strr => strr.slice(strr.indexOf("{"), strr.lastIndexOf("}") + 1))
          //        .filter(row=>{
          //          val jsonElem = stringToJsonConverter(row)
          //          val event_name= jsonElem.get("event_type").getAsString
          //          eventNames.contains(event_name)
          //         })

          val df = spark.read.json(jsonRDD).filter(col("event_type").isin(eventNames: _*))
          //          OPrinter.logAndPrint("No. of records  " + df.count())

          val schemaJson = spark.read.text("/dgn/lts/schema_checkpoints/schema_lts.txt").toDF("col")
          //          schemaJson.printSchema

          val splitDF = schemaJson.withColumn("column_name", functions.split($"col", ":")(0))
            .withColumn("column_type", functions.split($"col", ":")(1))
            .drop("col")
          //      splitDF.show(2,false)

          val colSchema = splitDF.collect().map { (row: Row) =>
            val column = row.getString(0)
            val datatype = row.getString(1)

            // Create a map for each row
            (column.trim(), datatype.trim())
          }.toMap

          //      colSchema.foreach(println)

          val jsonData = df.toJSON.rdd
          val repRDD= jsonData.repartition(numPartitions)
          //          OPrinter.logAndPrint("RDD[String] as :")
          //          jsonData.take(1).foreach(println)

          var retRDD = repRDD.mapPartitions(partition => {
              val connection = HbaseConnectionUtilProd.getConnection()
              val useridToGocidHbaseTbl = connection.getTable(TableName.valueOf(dgn_map_userid_gocid_tbl))
              val gocidToUseridHbaseTbl = connection.getTable(TableName.valueOf(dgn_map_gocid_userid_tbl))

              val newRDD = partition.map(row => {
                if (row != null && row.contains("event_type") && row.contains("ts") && row.contains("user_id") ) {
                  //                OPrinter.logAndPrint("For row: " + row)
                  val gson = new Gson()
                  val jsonObject: JsonObject = gson.fromJson(row, classOf[JsonObject])

                  val event_name = jsonObject.get("event_type").getAsString
                  val epoch_time = jsonObject.get("ts").getAsLong
                  val user_id = Option(jsonObject.getAsJsonPrimitive("user_id"))
                    .map(_.getAsString)
                    .getOrElse("")

                  val game_id = "Lucky Time Slots"

                  if(user_id == "") {
                    Notifier.sendEMail("dgn_lts@octro.com", s"Data packet doesn't contain user_id for \n : $row", "simran.maurya@octrotalk.com", "Action required : Couldn't process dgn-lts data")
                  }

                  val rowkey = user_id.trim()
                  var getUserData = (rowkey, useridToGocidHbaseTbl.get(new Get(Bytes.toBytes(rowkey))))

                  //                OPrinter.logAndPrint(s"Get data from hbase table $ref_user_id")
                  var gocid_from_hbase = ""
                  try {
                    gocid_from_hbase = Bytes.toString(getUserData._2.getValue(Bytes.toBytes("data"), Bytes.toBytes("goc_id"))).toString()
                  } catch {
                    case e: Exception => {
                      //                    OPrinter.logAndPrint("Inside catch")
                    }
                  }
                  //                OPrinter.logAndPrint(s"Get data from hbase table $gocid_from_hbase")

                  var hbasePut = new Put(Bytes.toBytes(user_id))
                  val event_time = Format.epochToDateTime(epoch_time)

                  val colList = mapperEventCol(event_name)

                  //            if(event_name.equalsIgnoreCase("level_up")){
                  var eventValueJson = new JsonObject
                  for (col <- colList) {
                    //                  OPrinter.logAndPrint(s"$col: $colSchema.get(col)")
                    val dataType = colSchema(col)
                    val extractedValue: Any = dataType match {
                      case "string" => {
                        Option(jsonObject.get(col)) match {
                          case Some(jsonElement) =>
                            val value = jsonElement.getAsString
                            eventValueJson.add(col, new JsonPrimitive(value))
                          case None =>
                            eventValueJson.add(col, JsonNull.INSTANCE)
                        }
                      }
                      case "long" => {
                        Option(jsonObject.get(col)) match {
                          case Some(jsonElement) =>
                            val value = jsonElement.getAsLong
                            eventValueJson.add(col, new JsonPrimitive(value))
                          case None =>
                            eventValueJson.add(col, JsonNull.INSTANCE)
                        }
                      }
                      case "boolean" => {
                        Option(jsonObject.get(col)) match {
                          case Some(jsonElement) =>
                            val value = jsonElement.getAsBoolean
                            eventValueJson.add(col, new JsonPrimitive(value))
                          case None =>
                            eventValueJson.add(col, JsonNull.INSTANCE)
                        }
                      }
                      case "double" => {
                        Option(jsonObject.get(col)) match {
                          case Some(jsonElement) =>
                            val value = jsonElement.getAsDouble
                            eventValueJson.add(col, new JsonPrimitive(value))
                          case None =>
                            eventValueJson.add(col, JsonNull.INSTANCE)
                        }
                      }
                      case "array" => {
                        Option(jsonObject.get(col)) match {
                          case Some(jsonElement) =>
                            val value = jsonElement.getAsJsonObject
                            eventValueJson.add(col, value)
                          case None =>
                            eventValueJson.add(col, JsonNull.INSTANCE)
                        }
                      }
                      case _ => {
                        //                      OPrinter.logAndPrint(s"for column $col else datatype: $dataType \n row $row")
                        throw new IllegalArgumentException(s"Unsupported data type: $dataType")
                      }
                    }

                  }
                  eventValueJson.add("epoch_time", new JsonPrimitive(epoch_time))
                  eventValueJson.add("event_time", new JsonPrimitive(event_time))
                  eventValueJson.add("user_id_numeric", new JsonPrimitive(user_id))

                  var goc_id = ""
                  if (gocid_from_hbase != "") {
                    //                  OPrinter.logAndPrint("Inside if line 251: " + gocid_from_hbase)
                    goc_id = gocid_from_hbase
                  }
                  else {
                    accum.add(1)

                    OPrinter.logAndPrint(s"Fetching gocid for $user_id")
                    val response = getGOCIDfromUSERID(user_id)
                    val outerJson = stringToJsonConverter(response)
                    //                  val innerJson = outerJson.get("body").getAsJsonObject
                    //                  val goc_id = innerJson.get("GOCID").getAsString
                    val innerJson = stringToJsonConverter(outerJson.get("body").getAsString)
                    //                  println("innerJson "+innerJson)
                    goc_id = innerJson.get("body").getAsJsonObject.get("GOCID").getAsString
                    //                  val goc_id= getGOCIDfromUSERID(ref_user_id)

                    if (goc_id != "") {
                      //                OPrinter.logAndPrint("Inside if line 262: " + goc_id)
                      hbasePut.addColumn("data".getBytes, "goc_id".getBytes, goc_id.toString().getBytes)
                      useridToGocidHbaseTbl.put(hbasePut)
                    }

                    // Now putting user_id corresponding to this goc_id in dgn_map_gocid_userid_tbl
                    var hbasePut2 = new Put(Bytes.toBytes(goc_id))
                    hbasePut2.addColumn("data".getBytes, "numeric_id".getBytes, user_id.toString().getBytes)
                    gocidToUseridHbaseTbl.put(hbasePut2)
                  }

                  val mainJson = new JsonObject
                  mainJson.addProperty("game_id", game_id)
                  mainJson.addProperty("event_time", event_time)
                  mainJson.addProperty("event_name", event_name)
                  mainJson.addProperty("user_id", goc_id)
                  mainJson.add("event_value", eventValueJson)

                  //                OPrinter.logAndPrint("Value of no. of API hits for this hour: " + no_of_hits)
                  (mainJson.toString, game_id, goc_id, event_time, event_name, eventValueJson.toString)
                  //            (event_name, event_time, game_id, hashed_user_id, eventValueJson.toString)
                }
                else {
                  Notifier.sendEMail("dgn_lts@octro.com", s"Data packet is missing some key for \n : $row", "simran.maurya@octrotalk.com", "Action required : Couldn't process dgn-lts data")
                  //            OPrinter.logAndPrint("Else row" + row)
                  ("", "", "", "", "", "")
                }
              })
              newRDD
            })
            .persist()

          OPrinter.logAndPrint(s"retRDD data count: ${retRDD.count()}")


          var empty_gocidRDD = retRDD
            .filter(row => {
              val jsoStr = stringToJsonConverter(row._1)
              jsoStr.has("user_id") && jsoStr.get("user_id").getAsString.isEmpty
            })


          if (!empty_gocidRDD.isEmpty()) {
            val empty_gocid_count = empty_gocidRDD.count()
            val sample_rec = empty_gocidRDD.take(5).mkString("\n")
            OPrinter.logAndPrint(s"Didn't receive goc_id for these packets ${sample_rec}")
            Notifier.sendEMail("dgn_ovs@octro.com", s"Didn't receive goc_id for these no. of records ${empty_gocid_count} \n Sample records are: \n ${sample_rec}", "simran.maurya@octrotalk.com", "Action required : Couldn't get gocid for dgn-ovs data")
            spark.sparkContext.stop()
          }

          var non_emptyRDD = retRDD.filter(row => {
            val jsoStr = stringToJsonConverter(row._1)
            jsoStr.has("user_id") && jsoStr.get("user_id").getAsString.nonEmpty
          })

          OPrinter.logAndPrint(s"Final data count: ${non_emptyRDD.count()}")

          //          OPrinter.logAndPrint(s"Pushing $hour hour data to kafka topic $dgnLtsTopic")
          //          OKafkaProducer.pushToTopic(dgnLtsTopic, non_emptyRDD.map(x => x._1))
          //          OPrinter.logAndPrint(s"Data saved to kafka topic $dgnLtsTopic")


          val lastReadDateHour= paths.head.split("prod/")(1).slice(0, 13)
          //          val timestamp = getCurrentTimestamp

          OPrinter.logAndPrint(s"Writing data on hdfs for $lastReadDateHour at $getCurrentTimestamp")

          non_emptyRDD.map(x => x._1).saveAsTextFile(s"/tmp/simran/lts_data/$lastReadDateHour/$getCurrentTimestamp")
          OPrinter.logAndPrint(s"Data saved on hdfs")

          // Extract timestamp and hash from the list of tuples
          val timestampAndHashList = pathsList.map { case (_, hash, timestamp) => (hash, timestamp) }

          import spark.implicits._
          val df_ = spark.createDataFrame(timestampAndHashList).toDF("hashed_path","datetime")         // df.show(10,false)
          df_.write.mode(SaveMode.Append).parquet("/tmp/simran/processed_paths")

          //          OPrinter.logAndPrint("Writing last read path on HDFS" + "!!")
          //          writeLastReadPathOnHDFS(s"$lastReadDateHour", "/dgn/lts/schema_checkpoints", spark)

          retRDD.unpersist()

          OPrinter.logAndPrint(s"Value of no. of API hits for $lastReadDateHour: ${accum.value}")

        })

      }
      catch {
        case e: Exception => {
          OPrinter.logAndPrint("Spark Context Stopped")
          spark.sparkContext.stop()
          //          val stackTrace = "com.octro.start.main.StartMain :: " + ExceptionUtils.getStackTrace(e) + "<br/><br/>"
          //          Notifier.sendEMail("dgn_ovs@octro.com", s"Error occurred in dgn_ovs connector for date $start_date hour $hour " + "\n<br/>" + e.getMessage + "\n<br/>stackTrace = " + stackTrace, "simran.maurya@octrotalk.com", "Action required : Couldn't process dgn-ovs data")
          Notifier.sendEMail("dgn_lts@octro.com", s"Error occurred in dgn_lts connector" + e.getMessage, "simran.maurya@octrotalk.com", "Action required : Couldn't process dgn-lts data")
          OPrinter.logAndPrint(e.getMessage)
        }
      }
    }
    "\n done"
  }

  //  def getPathsToRead(lastHourRead:String, currDay: DateTime, nextDay: String, spark: SparkSession): List[String] = {
  //
  //    val hadoopConf = new org.apache.hadoop.conf.Configuration()
  //    hadoopConf.set("fs.defaultFS", "hdfs://octropc")
  //    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
  //    val formatter = DateTimeFormat.forPattern("yyyy/MM/dd")
  //    val toRead= currDay.toString(formatter)
  //    println("toRead", toRead)
  //    // val fs = FileSystem.get(new Configuration())
  //    val currentList = hdfs.listStatus(new Path(s"/dgn/lts/raw_events/events/prod/$toRead"))
  //
  //    // Fetch all paths from todayList and add it into a list
  //    val filePaths =ListBuffer[String]()
  //    currentList.foreach(x=>{
  //      filePaths += x.getPath.toString
  //    })
  //
  //    // If lastHourRead is 22 or 23, then we will fetch paths from next day  also along with the
  //    if(lastHourRead.substring(lastHourRead.length-2) == "22" || lastHourRead.substring(lastHourRead.length-2) =="23"){
  //      val currentPlusOneList = hdfs.listStatus(new Path(s"/dgn/lts/raw_events/events/prod/$nextDay"))
  //      currentPlusOneList.foreach(x=>{
  //        //   println(x.getPath)
  //        filePaths += x.getPath.toString
  //      })
  //    }
  //
  //    val fileList = filePaths.toList
  //    fileList.foreach(println)
  //    println(fileList.size)
  //
  //    //Fetch list of file paths after lastReadHour
  //    val fileListAfterSubstring = fileList.dropWhile(!_.contains(lastHourRead)).drop(1)
  //    println("Print the list of file paths after lastReadHour")
  //    fileListAfterSubstring.foreach(println)
  //    println(fileListAfterSubstring.size)
  //    if(fileListAfterSubstring.size < 2){
  //      Notifier.sendEMail("dgn_lts@octro.com", s"There must be some lag on DGN-LTS data copying on HDFS " , "simran.maurya@octrotalk.com,amar.singh@octro.com", "Action required : Check for lag in DGN_LTS paths on hdfs")
  //      OPrinter.logAndPrint("Spark Context Stopped")
  //      spark.sparkContext.stop()
  //    }
  //    //    println(fileListAfterSubstring(0))
  //
  //    val todayDay = new DateTime()
  //    val currentHour = todayDay.toString(Format.hourFmt).replace("-","/")
  //
  //    // Filter out the currentHourPath from list of all paths to be read (as data must not be prepared completely for currentHou)
  //    val filterList= fileListAfterSubstring.dropRight(1).filter(!_.contains(currentHour))     //fileList.dropWhile(!_.contains(str)).drop(1).filter(!_.contains(currentHourPath))
  //    println("FilterList:")
  //    filterList.foreach(println)
  //    if(filterList.size < 1){
  //      Notifier.sendEMail("dgn_lts@octro.com", s"There must be some lag on DGN-LTS data copying on HDFS " , "simran.maurya@octrotalk.com", "Action required : Check for lag in DGN_LTS paths on hdfs")
  //      OPrinter.logAndPrint("Spark Context Stopped")
  //      spark.sparkContext.stop()
  //    }
  //    //    val lastPathToRead= filterList(filterList.length-1)
  //    //    println("\n",lastPathToRead)
  //    //    println(lastPathToRead.substring(lastPathToRead.size-13))
  //
  //    filterList
  //  }


  def writeLastReadPathOnHDFS(hour: String, path: String, sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    val hourDf = List(hour).toDF("value").coalesce(1)
    //    hourDf.write.mode(SaveMode.Overwrite).text(s"${path}/dgn_ovs_lastReadHour.txt")
    hourDf.write.mode(SaveMode.Overwrite).text(s"${path}/dgn_lts_lastReadHour.txt")
  }

  def readLastPathOnHDFS(path: String, sparkSession: SparkSession) = {
    import sparkSession.implicits._
    //    val hourDf = sparkSession.read.text(s"${path}/dgn_ovs_lastReadHour.txt").collect().head.getString(0)
    val hourDf = sparkSession.read.text(s"${path}/dgn_lts_lastReadHour.txt").collect().head.getString(0)
    hourDf
  }



  def getGOCIDfromUSERID(userId: String): String = {
    val gocidEndPoint = "https://nczs65s12a.execute-api.us-east-1.amazonaws.com/v1/userprovision/guestReg"
    //    val gocidEndPoint = "http://serv117.octro.net:4004/userprovision/guestReg" //"http://serv117.octro.com:4004/userprovision/guestReg"
    val action = "guestRegister"
    val data_from = "dwbsid"
    val guestId = userId
    val appid = "20240221mrpi" // LTS app_id

    try {
      val response = Http(gocidEndPoint)
        .header("content-type", "application/json")
        .postData(s"""{"action":"guestRegister","appid":"20240221mrpi","guestId":"$guestId","data_from":"dwbsid"}""")
        .asString
      //      val response = Http(gocidEndPoint)
      //        .postForm
      //        .param("action", action)
      //        .param("guestId", guestId)
      //        .param("appid", appid)
      //        .param("data_from", data_from)
      //        .asString
      //    println("API response body = " + response.body)
      //    println("API response code = " + response.code)
      val res_body = response.body
      res_body
    }
    catch {
      case e: Exception => {
        val stackTrace = "com.octro.start.main.StartMain :: " + ExceptionUtils.getStackTrace(e) + "<br/><br/>"
        Notifier.sendEMail("dgn_lts@octro.com", s"Error occurred while fetching gocid for userid $guestId  " + "\n<br/>" + e.getMessage + "\n<br/>stackTrace = " + stackTrace, "simran.maurya@octrotalk.com", "Action required : Couldn't fetch gocid from api")
        ""
      }
    }
    //    println("res_body type : "+res_body.getClass().getSimpleName())
  }

  def epochToPacificTime(epoch: Long): String = {
    // Convert epoch time to DateTime
    val dateTime = new DateTime(epoch, DateTimeZone.UTC)

    // Set the time zone to Chicago
    val pacificTimeZone = DateTimeZone.forID("America/Los_Angeles")
    val pacificDateTime = dateTime.withZone(pacificTimeZone)

    // Format DateTime as a string
    val formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
    val pacificTimeString = formatter.print(pacificDateTime)
    val dateTimeString = formatter.print(dateTime) // UTC String

    pacificTimeString
  }

  private def stringToJsonConverter(json: String): JsonObject = {
    val jsonParser = new JsonParser()
    val json_element = jsonParser.parse(json)
    val json_data = json_element.getAsJsonObject
    json_data
  }

}




