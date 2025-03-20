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

import scala.collection.mutable.ListBuffer
//import com.octro.utils.OKafkaProducer
import org.apache.commons.lang.exception.ExceptionUtils
import com.google.gson.JsonNull
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Append, Get, HTable, Increment, Put, Result, Scan, Table}
import java.security.MessageDigest


object DGN_OVS_Events_Main {
  val dgnOvsTopic = "dgn_events" //"DGN-Events"
  val dgn_map_userid_gocid_tbl = "dgn_ovs_numid_gocid"
  val dgn_map_gocid_userid_tbl = "dgn_ovs_gocid_hashedid"
  val dgn_map_hashedid_gocid_tbl = "dgn_ovs_hashedid_gocid"

  def main(args: Array[String]): Unit = {
    OPrinter.logAndPrint("*************** Welcome to DGN Connector On 29th Mar'24  ***************")


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
    import spark.implicits._

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

    //    val hourPaths = List[String]("00","01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23")
    val eventNames = List("transaction", "level_up", "install", "start_session", "worlds", "spin", "facebook_connect")

    val transnCol = List("purchase_count", "price", "session_id", "event_id", "platform_id")
    val levelUpCol = List("session_id", "level", "vip_level", "event_id", "platform_id")
    val installCol = List("session_id", "event_id", "platform_id")
    val startSesCol = List("version", "balance", "bingo_balls_balance", "platform_id", "blackdiamond_elite_active", "session_id", "event_id", "level", "vip_level")
    val worldsCol = List("session_id", "last_unlocked_world", "event_id", "platform_id")
    val spinCol = List("bet_amount", "event_id", "session_id", "platform_id")
    val facebookConnectCol = List("session_id", "fb_id", "event_id")

    var mapperEventCol: Map[String, List[String]] = Map("transaction" -> transnCol, "level_up" -> levelUpCol, "install" -> installCol, "start_session" -> startSesCol, "worlds" -> worldsCol, "spin" -> spinCol, "facebook_connect" -> facebookConnectCol)

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
      if (event.equalsIgnoreCase("worlds")) {
        colList = colList + (event -> worldsCol)
      }
      if (event.equalsIgnoreCase("spin")) {
        colList = colList + (event -> spinCol)
      }
      if (event.equalsIgnoreCase("facebook_connect")) {
        colList = colList + (event -> facebookConnectCol)
      }
    }


    try {

      val lastHourRead = readLastPathOnHDFS("/dgn/ovs/schema_checkpoints", spark).trim //.first().getString(0)

      OPrinter.logAndPrint(s"lastHourRead is $lastHourRead")

      val formatter = DateTimeFormat.forPattern("yyyy/MM/dd")
      val currDay = DateTime.parse(lastHourRead.substring(0,lastHourRead.length-3), formatter)
      println(currDay)
      val nextDay= currDay.plusDays(1).toString(formatter)
      println(nextDay)

      val paths= getPathsToRead(lastHourRead, currDay, nextDay, spark)

      val status = hourwiseDataReader(paths, spark, eventNames, mapperEventCol, numPartitions)

      OPrinter.logAndPrint(s"Data proccessed for " + status)
    }
    catch{
      case e: Exception => {
        OPrinter.logAndPrint("Spark Context Stopped")
        spark.sparkContext.stop()
        Notifier.sendEMail("dgn_ovs@octro.com", s"Error occurred " + e.getMessage, "simran.maurya@octrotalk.com", "Action required : Couldn't process dgn-ovs data")
        OPrinter.logAndPrint(e.getMessage)
      }
    }

  }

  def hourwiseDataReader(hourPathToRead: List[String], spark: SparkSession, eventNames: List[String], mapperEventCol: Map[String, List[String]], numPartitions: Int): String = {
    import spark.implicits._

    if (hourPathToRead.size != 0) {
      try {
        hourPathToRead.foreach(hour => {

          //          var no_of_hits = 0

          OPrinter.logAndPrint(s"Reading data for path $hour/*")
          //          (s"$hour/*")
          val jsonRDD = spark.sparkContext.textFile("/dgn/ovs/raw_events/ovs/events/prod/2024/05/29/10/*")
            .flatMap(json => json.split("\\*##\\*")).map(strr => strr.slice(strr.indexOf("{"), strr.lastIndexOf("}") + 1)).persist()

          val df = spark.read.json(jsonRDD).filter(col("event_type").isin(eventNames: _*))
          OPrinter.logAndPrint("No. of records  " + df.count())

          val schemaJson = spark.read.text("/dgn/ovs/schema_checkpoints/schema.txt").toDF("col")
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
              val hashedidToGocidHbaseTbl = connection.getTable(TableName.valueOf(dgn_map_hashedid_gocid_tbl))

              val newRDD = partition.map(row => {
                if (row != null && row.contains("event_type") && row.contains("ts") && row.contains("user_id") ) {
                  //                OPrinter.logAndPrint("For row: " + row)
                  val gson = new Gson()
                  val jsonObject: JsonObject = gson.fromJson(row, classOf[JsonObject])

                  val event_name = jsonObject.get("event_type").getAsString
                  val epoch_time = jsonObject.get("ts").getAsLong
                  val hashed_user_id = Option(jsonObject.getAsJsonPrimitive("user_id"))
                    .map(_.getAsString)
                    .getOrElse("")
                  val ref_user_id = Option(jsonObject.getAsJsonPrimitive("numeric_user_id"))
                    .map(_.getAsString)
                    .getOrElse("")
                  val game_id = "Old Vegas Slots"

                  if(hashed_user_id == "") {
                    Notifier.sendEMail("dgn_ovs@octro.com", s"Data packet doesn't contain user_id for \n : $row", "simran.maurya@octrotalk.com", "Action required : Couldn't process dgn-ovs data")
                  }

                  val rowkey = hashed_user_id.trim()
                  var getUserData = (rowkey, hashedidToGocidHbaseTbl.get(new Get(Bytes.toBytes(rowkey))))

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

                  var hbasePut = new Put(Bytes.toBytes(hashed_user_id))
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
                  eventValueJson.add("user_id_hashed", new JsonPrimitive(hashed_user_id))
                  eventValueJson.add("user_id_numeric", new JsonPrimitive(ref_user_id))


                  var goc_id = ""
                  if (gocid_from_hbase != "") {
                    //                  OPrinter.logAndPrint("Inside if line 251: " + gocid_from_hbase)
                    goc_id = gocid_from_hbase
                  }
                  else {
                    //                  OPrinter.logAndPrint("inside else")
                    //                  no_of_hits = no_of_hits + 1
                    val response = getGOCIDfromUSERID(hashed_user_id)
                    val outerJson = stringToJsonConverter(response)

                    val innerJson = stringToJsonConverter(outerJson.get("body").getAsString)
                    //                  println("innerJson "+innerJson)
                    val goc_id = innerJson.get("body").getAsJsonObject.get("GOCID").getAsString
                    //                  val goc_id= getGOCIDfromUSERID(ref_user_id)

                    if (goc_id != "") {
                      //                OPrinter.logAndPrint("Inside if line 262: " + goc_id)
                      hbasePut.addColumn("data".getBytes, "goc_id".getBytes, goc_id.toString().getBytes)
                      hbasePut.addColumn("data".getBytes, "numeric_id".getBytes, ref_user_id.toString().getBytes)
                      //              useridToGocidHbaseTbl.put(hbasePut)
                      hashedidToGocidHbaseTbl.put(hbasePut)
                    }

                    // Now putting user_id corresponding to this goc_id in dgn_map_gocid_userid_tbl
                    var hbasePut2 = new Put(Bytes.toBytes(goc_id))
                    hbasePut2.addColumn("data".getBytes, "numeric_id".getBytes, ref_user_id.toString().getBytes)
                    hbasePut2.addColumn("data".getBytes, "hashed_id".getBytes, hashed_user_id.toString().getBytes)
                    gocidToUseridHbaseTbl.put(hbasePut2)

                    //            var hbasePut3 = new Put(Bytes.toBytes(hashed_user_id))
                    if(ref_user_id !=""){
                      var hbasePut3 = new Put(Bytes.toBytes(ref_user_id))
                      hbasePut3.addColumn("data".getBytes, "hashed_id".getBytes, hashed_user_id.toString().getBytes)
                      hbasePut3.addColumn("data".getBytes, "goc_id".getBytes, goc_id.toString().getBytes)
                      useridToGocidHbaseTbl.put(hbasePut3)
                    }
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
                  Notifier.sendEMail("dgn_ovs@octro.com", s"Data packet is missing some key for \n : $row", "simran.maurya@octrotalk.com", "Action required : Couldn't process dgn-ovs data")
                  //            OPrinter.logAndPrint("Else row" + row)
                  ("", "", "", "", "", "")
                }
              })
              newRDD
            })
            .persist()

          OPrinter.logAndPrint(s"retRDD data count: ${retRDD.count()}")



          var empty_gocidRDD = retRDD
            //            .filter(x => x != null && x._1 != null && x._1 != "" && x._2 != null && x._2 != "")
            .filter(row => {
              val jsoStr = stringToJsonConverter(row._1)
              jsoStr.has("user_id") && jsoStr.get("user_id").getAsString.isEmpty
            })




          if (!empty_gocidRDD.isEmpty()) {
            val empty_gocid_count= empty_gocidRDD.count()
            val sample_rec= empty_gocidRDD.take(5).mkString("\n")
            OPrinter.logAndPrint(s"Didn't receive goc_id for these packets ${sample_rec}")
            Notifier.sendEMail("dgn_ovs@octro.com", s"Didn't receive goc_id for these no. of records ${empty_gocid_count} \n Sample records are: \n ${sample_rec}" , "simran.maurya@octrotalk.com", "Action required : Couldn't get gocid for dgn-ovs data")
            spark.sparkContext.stop()
          }

          var non_emptyRDD = retRDD.filter(row => {
            val jsoStr = stringToJsonConverter(row._1)
            jsoStr.has("user_id") && jsoStr.get("user_id").getAsString.nonEmpty
          })

          OPrinter.logAndPrint(s"Final data count: ${non_emptyRDD.count()}")

          //          OPrinter.logAndPrint(s"Pushing $hour hour data to kafka topic $dgnOvsTopic")
          //          OKafkaProducer.pushToTopic(dgnOvsTopic, non_emptyRDD.map(x => x._1))
          //          OPrinter.logAndPrint(s"Data saved to kafka topic $dgnOvsTopic")

          val lastReadDateHour= hour.substring(hour.size-13)

          //          OPrinter.logAndPrint(s"Writing data on hdfs for $lastReadDateHour")
          //          non_emptyRDD.map(x => x._1).saveAsTextFile(s"/dgn/ovs/kafka_events/$lastReadDateHour")
          //          OPrinter.logAndPrint(s"Data saved on hdfs")

          OPrinter.logAndPrint("Writing last read path on HDFS" + "!!")
          writeLastReadPathOnHDFS(s"$lastReadDateHour", "/dgn/ovs/schema_checkpoints", spark)
          //          retRDD.unpersist()
          retRDD.unpersist()
        })
      }
      catch {
        case e: Exception => {
          OPrinter.logAndPrint("Spark Context Stopped")
          spark.sparkContext.stop()
          //          val stackTrace = "com.octro.start.main.StartMain :: " + ExceptionUtils.getStackTrace(e) + "<br/><br/>"
          //          Notifier.sendEMail("dgn_ovs@octro.com", s"Error occurred in dgn_ovs connector for date $start_date hour $hour " + "\n<br/>" + e.getMessage + "\n<br/>stackTrace = " + stackTrace, "simran.maurya@octrotalk.com", "Action required : Couldn't process dgn-ovs data")
          Notifier.sendEMail("dgn_ovs@octro.com", s"Error occurred in dgn_ovs connector" + e.getMessage, "simran.maurya@octrotalk.com", "Action required : Couldn't process dgn-ovs data")
          OPrinter.logAndPrint(e.getMessage)
        }
      }
    }
    "\n done"
  }

  def getPathsToRead(lastHourRead:String, currDay: DateTime, nextDay: String, spark: SparkSession): List[String] = {

    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    hadoopConf.set("fs.defaultFS", "hdfs://octropc")
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    val formatter = DateTimeFormat.forPattern("yyyy/MM/dd")
    val toRead= currDay.toString(formatter)
    println("toRead", toRead)
    // val fs = FileSystem.get(new Configuration())
    val currentList = hdfs.listStatus(new Path(s"/dgn/ovs/raw_events/ovs/events/prod/$toRead"))

    // Fetch all paths from todayList and add it into a list
    val filePaths =ListBuffer[String]()
    currentList.foreach(x=>{
      filePaths += x.getPath.toString
    })

    // If lastHourRead is 22 or 23, then we will fetch paths from next day  also along with the
    if(lastHourRead.substring(lastHourRead.length-2) == "22" || lastHourRead.substring(lastHourRead.length-2) =="23"){
      val currentPlusOneList = hdfs.listStatus(new Path(s"/dgn/ovs/raw_events/ovs/events/prod/$nextDay"))
      currentPlusOneList.foreach(x=>{
        //   println(x.getPath)
        filePaths += x.getPath.toString
      })
    }

    val fileList = filePaths.toList
    fileList.foreach(println)
    println(fileList.size)

    //Fetch list of file paths after lastReadHour
    val fileListAfterSubstring = fileList.dropWhile(!_.contains(lastHourRead)).drop(1)
    println("Print the list of file paths after lastReadHour")
    fileListAfterSubstring.foreach(println)
    println(fileListAfterSubstring.size)
    if(fileListAfterSubstring.size < 2){
      Notifier.sendEMail("dgn_ovs@octro.com", s"There must be some lag on DGN-OVS data copying on HDFS " , "simran.maurya@octrotalk.com,amar.singh@octro.com", "Action required : Check for lag in DGN_OVS paths on hdfs")
      OPrinter.logAndPrint("Spark Context Stopped")
      spark.sparkContext.stop()
    }
    //    println(fileListAfterSubstring(0))

    val todayDay = new DateTime()
    val currentHour = todayDay.toString(Format.hourFmt).replace("-","/")

    // Filter out the currentHourPath from list of all paths to be read (as data must not be prepared completely for currentHou)
    val filterList= fileListAfterSubstring.filter(!_.contains(currentHour))     //fileList.dropWhile(!_.contains(str)).drop(1).filter(!_.contains(currentHourPath))
    println("FilterList:")
    filterList.foreach(println)
    if(filterList.size < 1){
      Notifier.sendEMail("dgn_ovs@octro.com", s"There must be some lag on DGN-OVS data copying on HDFS " , "simran.maurya@octrotalk.com", "Action required : Check for lag in DGN_OVS paths on hdfs")
      OPrinter.logAndPrint("Spark Context Stopped")
      spark.sparkContext.stop()
    }
    //    val lastPathToRead= filterList(filterList.length-1)
    //    println("\n",lastPathToRead)
    //    println(lastPathToRead.substring(lastPathToRead.size-13))

    filterList
  }


  def writeLastReadPathOnHDFS(hour: String, path: String, sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    val hourDf = List(hour).toDF("value").coalesce(1)
    //    hourDf.write.mode(SaveMode.Overwrite).text(s"${path}/dgn_ovs_lastReadHour.txt")
    hourDf.write.mode(SaveMode.Overwrite).text(s"${path}/dgn_ovs_lastReadHour.txt")
  }

  def readLastPathOnHDFS(path: String, sparkSession: SparkSession) = {
    import sparkSession.implicits._
    val hourDf = sparkSession.read.text(s"${path}/dgn_ovs_lastReadHour.txt").collect().head.getString(0)
    hourDf
  }



  def getGOCIDfromUSERID(userId: String): String = {
    val gocidEndPoint = "https://nczs65s12a.execute-api.us-east-1.amazonaws.com/v1/userprovision/guestReg"
    //"http://serv117.octro.net:4004/userprovision/guestReg" //"http://serv117.octro.com:4004/userprovision/guestReg"
    val action = "guestRegister"
    val data_from = "dwbsid"
    val guestId = userId
    val appid = "20240221VXIz" // OVS app_id

    try {
      val response = Http(gocidEndPoint)
        .header("content-type", "application/json")
        .postData(s"""{"action":"guestRegister","appid":"20240221VXIz","guestId":"$guestId","data_from":"dwbsid"}""")
        .asString

      //    println("API response body = " + response.body)
      //    println("API response code = " + response.code)
      val res_body = response.body
      res_body
    }
    catch {
      case e: Exception => {
        val stackTrace = "com.octro.start.main.StartMain :: " + ExceptionUtils.getStackTrace(e) + "<br/><br/>"
        Notifier.sendEMail("dgn_ovs@octro.com", s"Error occurred while fetching gocid for userid $guestId  " + "\n<br/>" + e.getMessage + "\n<br/>stackTrace = " + stackTrace, "simran.maurya@octrotalk.com", "Action required : Couldn't fetch gocid from api")
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



