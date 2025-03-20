package com.octro.start.main

import com.google.gson.{Gson, JsonObject, JsonParser, JsonPrimitive}
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode, SparkSession, functions}
import org.joda.time.{DateTime, DateTimeZone, Days}
import scalaj.http.Http
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.util.Bytes
import com.octro.utilities.{Format, Notifier, OPrinter}
import com.octro.utilities.HbaseConnectionUtilProd
import com.octro.utilities.OKafkaProducer
import org.joda.time.format.DateTimeFormat
//import com.octro.utils.OKafkaProducer
import org.apache.commons.lang.exception.ExceptionUtils
import com.google.gson.JsonNull
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Append, Get, HTable, Increment, Put, Result, Scan, Table}
import java.security.MessageDigest


object TestEvent {
  val dgnLtsTopic = "dgn_lts_events" //"DGN-Events"
  val dgn_map_userid_gocid_tbl = "dgn_lts_userid_gocid"
  val dgn_map_gocid_userid_tbl = "dgn_lts_gocid_userid"

  def main(args: Array[String]): Unit = {
    OPrinter.logAndPrint("*************** Welcome to DGN-LTS Connector On 21st Mar'24  ***************")


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
    import spark.implicits._

    //    val todayDay = new DateTime().withTimeAtStartOfDay()
    //    val start_date = "2024/01/23" //todayDay.minusDays(1).toString(Format.dayFmt).replace("-","/")

    // s"/tmp/amar/s3_dgn_test/ovs/events/prod/$start_date"
    //    val hourPaths = List[String]("19", "20", "21", "22", "23")
    val hourPaths = List[String]("02")
    val eventNames = List("transaction")
    //List("transaction", "level_up", "install", "start_session", "worlds", "spin", "facebook_connect")
    val transnCol = List("purchase_count", "price", "purchase_amount", "session_id", "event_id", "purchase_origin")
    val levelUpCol = List("session_id", "level", "vip_level", "event_id")
    val installCol = List("session_id", "event_id")
    val startSesCol = List("version", "balance", "platform_id", "session_id", "event_id", "level", "vip_level", "spins_per_spinner", "country_code", "avg_bet", "total_bet")
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


    val dateFormat = DateTimeFormat.forPattern("yyyy/MM/dd")

    // Define start and end dates
    val startDate = dateFormat.parseDateTime("2024/03/06")
    val endDate = dateFormat.parseDateTime("2024/03/06")

    //    val todayDay = new DateTime().withTimeAtStartOfDay()
    //    val currentDate = todayDay.minusDays(1).toString(Format.dayFmt).replace("-","/")


    // Calculate the number of days between the start and end dates
    val daysBetween = Days.daysBetween(startDate, endDate).getDays

    // Iterate over each day and print the date
    try {
      for (day <- 0 to daysBetween) {

        val currentDate = startDate.plusDays(day).toString(dateFormat)
        val dayPathToRead = s"/dgn/lts/raw_events/events/prod/$currentDate"
        println(s"Date: $currentDate")

        //      val status = hourwiseDataReader(hourPaths, spark, dayPathToRead, eventNames, mapperEventCol, currentDate)

        val lastReadPath = readLastPathOnHDFS("/dgn/lts/schema_checkpoints", spark) //.first().getString(0)
        val lastHourRead = lastReadPath.substring(lastReadPath.lastIndexOf("/") + 1).trim

        OPrinter.logAndPrint(s"lastHourRead is $lastHourRead")
        // Find the index of last read hour in the original hour list
        //        val index = hourPaths.zipWithIndex.filter(f => {
        //          lastHourRead.equals(f._1)
        //        }).head._2
        //      OPrinter.logAndPrint(s"index is $index")
        // Create a new list from that index onwards for hour paths to be read onwards
        //        val hourPathToRead = hourPaths.drop(index + 1)
        //        OPrinter.logAndPrint(s"Line 92 is $hourPathToRead")
        //        var status = ""
        //
        //        if (lastHourRead == "23") {
        //        OPrinter.logAndPrint(s"Inside if hourPathToRead is $hourPaths")
        val status = hourwiseDataReader(hourPaths, spark, dayPathToRead, eventNames, mapperEventCol, currentDate, numPartitions)
        //        }
        //        else {
        //          //        OPrinter.logAndPrint(s"Inside else hourPathToRead is $hourPathToRead")
        //          status = hourwiseDataReader(hourPathToRead, spark, dayPathToRead, eventNames, mapperEventCol, currentDate, numPartitions)
        //        }
        OPrinter.logAndPrint(s"Data proccessed for $currentDate " + status)
              }
      }
      catch
      {
        case e: Exception => {
          OPrinter.logAndPrint("Spark Context Stopped")
          spark.sparkContext.stop()
          Notifier.sendEMail("dgn_lts@octro.com", s"Error occurred for date $current_date()" + e.getMessage, "simran.maurya@octrotalk.com", "Action required : Couldn't process dgn-lts data")
          OPrinter.logAndPrint(e.getMessage)
        }
      }

    }

    def hourwiseDataReader(hourPathToRead: List[String], spark: SparkSession, dayPathToRead: String, eventNames: List[String], mapperEventCol: Map[String, List[String]], start_date: String, numPartitions: Int): String = {
      import spark.implicits._

      if (hourPathToRead.size != 0) {
        try {
          hourPathToRead.foreach(hour => {

            var no_of_hits = 0

            OPrinter.logAndPrint(s"Reading data for path $dayPathToRead/$hour/*")
            val jsonRDD = spark.sparkContext.textFile(s"$dayPathToRead/$hour/*")
              .flatMap(json => json.split("\\*##\\*")).map(strr => strr.slice(strr.indexOf("{"), strr.lastIndexOf("}") + 1))

            val df1 = spark.read.json(jsonRDD).filter(col("event_type").isin(eventNames: _*))
            //          OPrinter.logAndPrint("No. of records  " + df.count())
            val aggData = df1.columns
              .filter(f => (f != "event_type"))
              .map(colName => first(col(colName)).alias(s"$colName"))

            val df = df1.groupBy("event_type").agg(aggData.head, aggData.tail: _*)

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
            val repRDD = jsonData.repartition(numPartitions)
            //          OPrinter.logAndPrint("RDD[String] as :")
            //          jsonData.take(1).foreach(println)

            var retRDD = repRDD.mapPartitions(partition => {
              val connection = HbaseConnectionUtilProd.getConnection()
              val useridToGocidHbaseTbl = connection.getTable(TableName.valueOf(dgn_map_userid_gocid_tbl))
              val gocidToUseridHbaseTbl = connection.getTable(TableName.valueOf(dgn_map_gocid_userid_tbl))

              val newRDD = partition.map(row => {
                if (row != null && row.contains("event_type") && row.contains("ts") && row.contains("user_id")) {
                  //                OPrinter.logAndPrint("For row: " + row)
                  val gson = new Gson()
                  val jsonObject: JsonObject = gson.fromJson(row, classOf[JsonObject])

                  val event_name = jsonObject.get("event_type").getAsString
                  val epoch_time = jsonObject.get("ts").getAsLong
                  val user_id = Option(jsonObject.getAsJsonPrimitive("user_id"))
                    .map(_.getAsString)
                    .getOrElse("")

                  val game_id = "Lucky Time Slots"

                  if (user_id == "") {
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
                    //                  OPrinter.logAndPrint("inside else")
                    no_of_hits = no_of_hits + 1
                    val response = getGOCIDfromUSERID(user_id)
                    val outerJson = stringToJsonConverter(response)
                    //                  val innerJson = outerJson.get("body").getAsJsonObject
                    //                  val goc_id = innerJson.get("GOCID").getAsString
                    val innerJson = stringToJsonConverter(outerJson.get("body").getAsString)
                    //                  println("innerJson "+innerJson)
                    val goc_id = innerJson.get("body").getAsJsonObject.get("GOCID").getAsString
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
              }).filter(x => x != null && x._1 != null && x._1 != "" && x._2 != null && x._2 != "")
              newRDD
            })
//              .filter(row => {
//              val jsoStr = stringToJsonConverter(row._1)
//              jsoStr.has("user_id") && jsoStr.get("user_id").getAsString.nonEmpty
//            })
            //            .coalesce(1).persist()

            OPrinter.logAndPrint(s"Row count for $hour " + retRDD.count())
            //          OPrinter.logAndPrint(s"Putting Data for hour $hour of day $start_date")
//            retRDD.map(x => x._1).take(10).foreach(println)

            OPrinter.logAndPrint(s"Pushing $hour hour data to kafka topic $dgnLtsTopic")
            OKafkaProducer.pushToTopic(dgnLtsTopic, retRDD.map(x => x._1))
            OPrinter.logAndPrint(s"Data saved to kafka topic $dgnLtsTopic")

            retRDD.map(x => x._1).take(10).foreach(println)

            //          OPrinter.logAndPrint("Writing last read path on HDFS" + "!!")
            //          writeLastReadPathOnHDFS(s"/$hour", "/tmp/simran", spark)

            //          OPrinter.logAndPrint(s"Writing data on hdfs for date $start_date hour $hour")
            //          retRDD.map(x => x._1).saveAsTextFile(s"/dgn/lts/kafka_events/$start_date/$hour")
            //          OPrinter.logAndPrint(s"Data saved on hdfs")
            //          retRDD.unpersist()
//            df.unpersist()

          })
        }
        catch {
          case e: Exception => {
            OPrinter.logAndPrint("Spark Context Stopped")
            spark.sparkContext.stop()
            //          val stackTrace = "com.octro.start.main.StartMain :: " + ExceptionUtils.getStackTrace(e) + "<br/><br/>"
            //          Notifier.sendEMail("dgn_ovs@octro.com", s"Error occurred in dgn_ovs connector for date $start_date hour $hour " + "\n<br/>" + e.getMessage + "\n<br/>stackTrace = " + stackTrace, "simran.maurya@octrotalk.com", "Action required : Couldn't process dgn-ovs data")
            Notifier.sendEMail("dgn_lts@octro.com", s"Error occurred in dgn_lts connector for date $start_date" + e.getMessage, "simran.maurya@octrotalk.com", "Action required : Couldn't process dgn-lts data")
            OPrinter.logAndPrint(e.getMessage)
          }
        }
      }
      "\n done"
    }


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
      val gocidEndPoint = "http://serv117.octro.net:4004/userprovision/guestReg" //"http://serv117.octro.com:4004/userprovision/guestReg"
      val action = "guestRegister"
      val data_from = "dwbsid"
      val guestId = userId
      val appid = "20240221VXIz" // LTS app_id

      try {
        val response = Http(gocidEndPoint)
          .postForm
          .param("action", action)
          .param("guestId", guestId)
          .param("appid", appid)
          .param("data_from", data_from)
          .asString
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



