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


object LTS_correction {
  val dgnLtsTopic = "dgn_events" //"DGN-Events"
  val dgn_map_userid_gocid_tbl = "dgn_lts_userid_gocid"
  val dgn_map_gocid_userid_tbl = "dgn_lts_gocid_userid"

  def main(args: Array[String]): Unit = {
    OPrinter.logAndPrint("*************** Welcome to DGN-LTS Connector On 4th Jun'24  ***************")


    val conf = new SparkConf()
      .set("spark.streaming.backpressure.enabled", "true")
      .set("spark.streaming.kafka.maxRatePerPartition", "50") //100000 //50000 //25000  *60 (duration)
      .set("spark.ui.port", "4545")
      .set("spark.executor.memoryOverhead", "100m")
      .set("spark.hadoop.fs.defaultFS", "hdfs://octropc")

    val sc: SparkContext = new SparkContext(conf)
    val spark = new SQLContext(sc).sparkSession
    println("Application id=" + sc.applicationId)

    val hourPaths = List[String]("00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23")

    val eventNames = List("transaction", "level_up", "install", "start_session", "facebook_connect", "spin")
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
    val startDate = dateFormat.parseDateTime("2024/03/26")
    val endDate = dateFormat.parseDateTime("2024/03/26")


    // Calculate the number of days between the start and end dates
    val daysBetween = Days.daysBetween(startDate, endDate).getDays

    // Iterate over each day and print the date
    try {

      for (day <- 0 to daysBetween) {

        val currentDate = startDate.plusDays(day).toString(dateFormat)
        val dayPathToRead = s"/dgn/lts/raw_events/events/prod/$currentDate"
        println(s"Date: $currentDate")

        //     val status = hourwiseDataReader(hourPaths, spark, dayPathToRead, eventNames, mapperEventCol, currentDate)

        val lastReadPath = readLastPathOnHDFS("/tmp/simran", spark) //.first().getString(0)
        val lastHourRead = lastReadPath.substring(lastReadPath.lastIndexOf("/") + 1).trim

        OPrinter.logAndPrint(s"lastHourRead is $lastHourRead")
        // Find the index of last read hour in the original hour list
        val index = hourPaths.zipWithIndex.filter(f => {
          lastHourRead.equals(f._1)
        }).head._2
        //      OPrinter.logAndPrint(s"index is $index")
        // Create a new list from that index onwards for hour paths to be read onwards
        val hourPathToRead = hourPaths.drop(index + 1)
        OPrinter.logAndPrint(s"Line 92 is $hourPathToRead")
        var status = ""

        if (lastHourRead == "23") {
          //        OPrinter.logAndPrint(s"Inside if hourPathToRead is $hourPaths")
          status = hourwiseDataReader(hourPaths, spark, dayPathToRead, eventNames, mapperEventCol, currentDate, sc)
        }
        else {
          //        OPrinter.logAndPrint(s"Inside else hourPathToRead is $hourPathToRead")
          status = hourwiseDataReader(hourPathToRead, spark, dayPathToRead, eventNames, mapperEventCol, currentDate, sc)
        }
        OPrinter.logAndPrint(s"Data proccessed for $currentDate " + status)
      }
    }
    catch {
      case e: Exception => {
        OPrinter.logAndPrint("Spark Context Stopped")
        spark.sparkContext.stop()
        Notifier.sendEMail("dgn_lts@octro.com", s"Error occurred for date $current_date()" + e.getMessage, "simran.maurya@octrotalk.com", "Action required : Couldn't process dgn-lts data")
        OPrinter.logAndPrint(e.getMessage)
      }
    }

  }

  def hourwiseDataReader(hourPathToRead: List[String], spark: SparkSession, dayPathToRead: String, eventNames: List[String], mapperEventCol: Map[String, List[String]], start_date: String, sc: SparkContext): String = {
    import spark.implicits._

    if (hourPathToRead.size != 0) {
      try {
        hourPathToRead.foreach(hour => {

          val accum = spark.sparkContext.longAccumulator("api_hits")

          OPrinter.logAndPrint(s"Reading data for path $dayPathToRead/$hour/*")

          val jsonRDD = spark.sparkContext.textFile(s"$dayPathToRead/$hour/*")
            .flatMap(json => json.split("\\*##\\*")).map(strr => strr.slice(strr.indexOf("{"), strr.lastIndexOf("}") + 1))
            .filter(_.nonEmpty)

          val df = spark.read.json(jsonRDD).filter(col("event_type").isin(eventNames: _*)).repartition(20, col("user_id"))
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

          val jsonData = df.toJSON.rdd
          val repRDD = jsonData


          var retRDD = repRDD.mapPartitions(partition => {
            val connection = HbaseConnectionUtilProd.getConnection()
            val useridToGocidHbaseTbl = connection.getTable(TableName.valueOf(dgn_map_userid_gocid_tbl))
            val gocidToUseridHbaseTbl = connection.getTable(TableName.valueOf(dgn_map_gocid_userid_tbl))
            val gocIdMap: Map[String, String] = processGocIDMap(partition, useridToGocidHbaseTbl, gocidToUseridHbaseTbl, sc)

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
                  Notifier.sendEMail("dgn_lts_hist@octro.com", s"Data packet doesn't contain user_id for \n : $row", "simran.maurya@octrotalk.com", "Action required : Couldn't process dgn-lts data")
                }

//                val rowkey = user_id.trim()
//                var getUserData = (rowkey, useridToGocidHbaseTbl.get(new Get(Bytes.toBytes(rowkey))))
//
//                var gocid_from_hbase = ""
//                try {
//                  gocid_from_hbase = Bytes.toString(getUserData._2.getValue(Bytes.toBytes("data"), Bytes.toBytes("goc_id"))).toString()
//                } catch {
//                  case e: Exception => {
//                  }
//                }

                val event_time = Format.epochToDateTime(epoch_time)

                val colList = mapperEventCol(event_name)

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

//                var goc_id = ""
//                if (gocid_from_hbase != "") {
//                  goc_id = gocid_from_hbase
//                }
//                else {
//                  accum.add(1)
//                  OPrinter.logAndPrint(s"Fetching gocid for $user_id")
//
//                  val response = getGOCIDfromUSERID(user_id)
//                  val outerJson = stringToJsonConverter(response)
//                  //                  val innerJson = outerJson.get("body").getAsJsonObject
//                  //                  val goc_id = innerJson.get("GOCID").getAsString
//                  val innerJson = stringToJsonConverter(outerJson.get("body").getAsString)
//                  //                  println("innerJson "+innerJson)
//                  goc_id = innerJson.get("body").getAsJsonObject.get("GOCID").getAsString
//                  //                  val goc_id= getGOCIDfromUSERID(ref_user_id)
//
//                  if (goc_id != "") {
//                    //                OPrinter.logAndPrint("Inside if line 262: " + goc_id)
//                    hbasePut.addColumn("data".getBytes, "goc_id".getBytes, goc_id.toString().getBytes)
//                    useridToGocidHbaseTbl.put(hbasePut)
//                  }
//
//                  // Now putting user_id corresponding to this goc_id in dgn_map_gocid_userid_tbl
//                  var hbasePut2 = new Put(Bytes.toBytes(goc_id))
//                  hbasePut2.addColumn("data".getBytes, "numeric_id".getBytes, user_id.toString().getBytes)
//                  gocidToUseridHbaseTbl.put(hbasePut2)
//                }

                val goc_id = gocIdMap(user_id)

                val mainJson = new JsonObject
                mainJson.addProperty("game_id", game_id)
                mainJson.addProperty("event_time", event_time)
                mainJson.addProperty("event_name", event_name)
                mainJson.addProperty("user_id", goc_id)
                mainJson.add("event_value", eventValueJson)

                (mainJson.toString, game_id, goc_id, event_time, event_name, eventValueJson.toString)
              }
              else {
                Notifier.sendEMail("dgn_lts@octro.com", s"Data packet is missing some key for \n : $row", "simran.maurya@octrotalk.com", "Action required : Couldn't process dgn-lts data")
                ("", "", "", "", "", "")
              }
            }) //.filter(x => x != null && x._1 != null && x._1 != "" && x._2 != null && x._2 != "")
            newRDD
          }).persist()

          //          OPrinter.logAndPrint(s"retRDD data count: ${retRDD.count()}")          //          OPrinter.logAndPrint(s"Putting Data for hour $hour of day $start_date")
          //          retRDD.map(x => x._1).take(1).foreach(println)

          var empty_gocidRDD = retRDD
            .filter(row => {
              val jsoStr = stringToJsonConverter(row._1)
              jsoStr.has("user_id") && jsoStr.get("user_id").getAsString.isEmpty
            })


          if (!empty_gocidRDD.isEmpty()) {
            val empty_gocid_count = empty_gocidRDD.count()
            val sample_rec = empty_gocidRDD.take(5).mkString("\n")
            OPrinter.logAndPrint(s"Didn't receive goc_id for these packets ${sample_rec}")
            Notifier.sendEMail("dgn_lts_hist@octro.com", s"Didn't receive goc_id for these no. of records ${empty_gocid_count} \n Sample records are: \n ${sample_rec}", "simran.maurya@octrotalk.com", "Action required : Couldn't get gocid for dgn-lts data")
            spark.sparkContext.stop()
          }

          var non_emptyRDD = retRDD.filter(row => {
            val jsoStr = stringToJsonConverter(row._1)
            jsoStr.has("user_id") && jsoStr.get("user_id").getAsString.nonEmpty
          })

          //          OPrinter.logAndPrint(s"Final data count: ${non_emptyRDD.count()}")

          OPrinter.logAndPrint(s"Pushing $hour hour data to kafka topic $dgnLtsTopic")
          OKafkaProducer.pushToTopic(dgnLtsTopic, non_emptyRDD.map(x => x._1))
          OPrinter.logAndPrint(s"Data saved to kafka topic $dgnLtsTopic")

          //          OPrinter.logAndPrint("Writing last read path on HDFS" + "!!")
          //          writeLastReadPathOnHDFS(s"/$hour", "/tmp/simran", spark)

          OPrinter.logAndPrint(s"Writing data on hdfs for date $start_date hour $hour")
          non_emptyRDD.map(x => x._1).saveAsTextFile(s"/tmp/simran/lts/kafka_events_new/$start_date/$hour")
          OPrinter.logAndPrint(s"Data saved on hdfs")

          OPrinter.logAndPrint("Writing last read path on HDFS" + "!!")
//          writeLastReadPathOnHDFS(s"$hour", "/tmp/simran", spark)

          retRDD.unpersist()


        })
      }
      catch {
        case e: Exception => {
          OPrinter.logAndPrint("Spark Context Stopped")
          spark.sparkContext.stop()
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
    hourDf.write.mode(SaveMode.Overwrite).text(s"${path}/dgn_lts_lastReadHour14Mar.txt")
  }

  def readLastPathOnHDFS(path: String, sparkSession: SparkSession) = {
    import sparkSession.implicits._
    //    val hourDf = sparkSession.read.text(s"${path}/dgn_ovs_lastReadHour.txt").collect().head.getString(0)
    val hourDf = sparkSession.read.text(s"${path}/dgn_lts_lastReadHour14Mar.txt").collect().head.getString(0)
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

  def processGocIDMap(partition: Iterator[String], useridToGocidHbaseTbl: Table, gocidToUseridHbaseTbl: Table, sc: SparkContext): Map[String, String] = {
    val userIdList = partition.map(row => {
      val jsonData = stringToJsonConverter(row)
      var user_id = "NA"
      if (jsonData.has("user_id")) {
        user_id = jsonData.get("user_id").getAsString
      }
      user_id.trim()
    }).toList.distinct

    if (userIdList.filter(_.equals("NA")).nonEmpty) {
      Notifier.sendEMail("dgn_lts_hist@octro.com", s"Data packet doesn't contain user_id", "simran.maurya@octrotalk.com", "Action required : Couldn't process dgn-lts data")
      sc.stop()
    }

    val user_gocid_mapper = userIdList.map(user_id => {

      var getUserData = (user_id, useridToGocidHbaseTbl.get(new Get(Bytes.toBytes(user_id))))
      var hbasePut = new Put(Bytes.toBytes(user_id))

      var gocid_from_hbase = ""
      try {
        gocid_from_hbase = Bytes.toString(getUserData._2.getValue(Bytes.toBytes("data"), Bytes.toBytes("goc_id"))).toString()
      } catch {
        case e: Exception => {
        }
      }

      var goc_id = ""
      if (gocid_from_hbase != "") {
        goc_id = gocid_from_hbase
      }
      else {
        OPrinter.logAndPrint(s"Fetching gocid from api $user_id")

        val response = getGOCIDfromUSERID(user_id)
        val outerJson = stringToJsonConverter(response)

        val innerJson = stringToJsonConverter(outerJson.get("body").getAsString)
        goc_id = innerJson.get("body").getAsJsonObject.get("GOCID").getAsString

        if (goc_id != "") {
          hbasePut.addColumn("data".getBytes, "goc_id".getBytes, goc_id.toString().getBytes)
          useridToGocidHbaseTbl.put(hbasePut)
        }

        // Now putting user_id corresponding to this goc_id in dgn_map_gocid_userid_tbl
        var hbasePut2 = new Put(Bytes.toBytes(goc_id))
        hbasePut2.addColumn("data".getBytes, "numeric_id".getBytes, user_id.toString().getBytes)
        gocidToUseridHbaseTbl.put(hbasePut2)
      }
      //      user_gocid_mapper = user_gocid_mapper + (user_id -> goc_id)
      (user_id, goc_id)
    }).toMap
    user_gocid_mapper
  }

}



