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
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.OutputStreamWriter

object DgnOvsEventsV2 {
  val dgnLtsTopic = "dgn_lts_events" //"DGN-Events"
  val dgn_map_userid_gocid_tbl = "dgn_lts_userid_gocid"
  val dgn_map_gocid_userid_tbl = "dgn_lts_gocid_userid"

  def main(args: Array[String]): Unit = {

    OPrinter.logAndPrint("*************** Welcome to DGN-LTS connector On 14th Jun'24  ***************")
    val conf = new SparkConf()
    val sc: SparkContext = new SparkContext(conf)
    val spark = new SQLContext(sc).sparkSession
    OPrinter.logAndPrint("Application id=" + sc.applicationId)

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
    val eventNames = List("transaction", "level_up", "install", "start_session", "facebook_connect", "spin", "end_session")
    val transnCol = List("purchase_count", "price", "purchase_amount", "session_id", "event_id", "purchase_origin", "local_price", "real_exchange_rate", "platform_id", "country_code")
    val levelUpCol = List("session_id", "level", "vip_level", "event_id")
    val installCol = List("session_id", "event_id")
    val startSesCol = List("version", "balance", "platform_id", "session_id", "event_id", "level", "vip_level", "spins_per_spinner", "country_code", "avg_bet", "total_bet", "device_type")
    val facebookConnectCol = List("session_id", "fb_id", "event_id")
    val spinCol = List("bet_amount", "event_id", "session_id", "slot_id", "slot_location", "level")
    val endSesCol = List("event_id", "session_id", "platform_id", "balance", "level", "vip_level", "last_activity_ts", "country_code") //tag

    var mapperEventCol: Map[String, List[String]] = Map("transaction" -> transnCol, "level_up" -> levelUpCol, "install" -> installCol, "start_session" -> startSesCol, "facebook_connect" -> facebookConnectCol, "spin" -> spinCol, "end_session" -> endSesCol)

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
      filteringAlreadyProcessedPath(dateMap)
      hourwiseDataReader(dateMap, spark, eventNames, mapperEventCol, numPartitions)
    }
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
          if (!result.isEmpty) {
            for (i <- 0 to 23) {
              val hour = f"$i%02d"
              val value = result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(hour))
              if (result.containsColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(hour))) {
                val processedPaths = Bytes.toString(value).split(',').toSet
                //println(processedPaths)
                val updatedFileList = dateMap(date)(i).map {
                  case (path, existence) => (path, processedPaths.contains(path))
                }
                dateMap(date) = dateMap(date).updated(i, updatedFileList)
              }
            }
          }
      }
      println("Hourmap updated Successfully")
    }
  def hourwiseDataReader(dateMap: mutable.Map[String, List[List[(String, Boolean)]]], spark: SparkSession, eventNames: List[String], mapperEventCol: Map[String, List[String]], numPartitions: Int): Unit = {
    import spark.implicits._
    val connection = HbaseConnectionUtilProd.getConnection()
    val tableName = "already_processed_paths"
    val table = connection.getTable(TableName.valueOf(tableName))
    println("Connection made Successfully for write");
    val accum = spark.sparkContext.longAccumulator("api_hits")
    try {
      val dateHourWisePaths: Map[(String, Int), List[String]] = dateMap.flatMap { case (date, hourlyLists) =>
        hourlyLists.zipWithIndex.flatMap { case (hourlyList, hour) =>
          val falsePaths: List[String] = hourlyList.asInstanceOf[List[(String, Boolean)]].collect { case (path: String, flag: Boolean) if !flag => path }
          if (falsePaths.nonEmpty) Some((date, hour) -> falsePaths) else None
        }
      }.toMap
      //println(dateHourWisePaths);
      dateHourWisePaths.foreach { case ((date, hour), paths) =>
        println("Size of path List that Need to Processed",paths.size)
        if (paths.nonEmpty) {
          println("hi")
          val formattedHour = f"$hour%02d" // Ensure "00", "01", etc.
          //println(s"Reading data for Date: $date, Hour: $formattedHour, Paths: $paths")
          val jsondf = spark.read.option("lineSep", "**##**").text(paths: _*)
          val df = jsondf.withColumn("event_type", get_json_object($"value", "$.event_type")).filter(col("event_type").isin(eventNames: _*))
          //df.show(false)
          val jsonData = df.toJSON.rdd
          println("check2")
          //jsonData.collect().foreach(println)
//          val schemaJson = spark.read.text("/data_sdc/projects/chirag/schema_checkpoints/schema.txt").toDF("col")
//          val splitDF = schemaJson.withColumn("column_name", functions.split($"col", ":")(0))
//            .withColumn("column_type", functions.split($"col", ":")(1))
//            .drop("col")
//          val colSchema = splitDF.collect().map { (row: Row) =>
//            val column = row.getString(0)
//            val datatype = row.getString(1)
//            (column.trim(), datatype.trim())
//          }.toMap
//          val repRDD = jsonData.repartition(numPartitions)
//          var retRDD = repRDD.mapPartitions(partition => {
//              /** ********* */
//              val connection = HbaseConnectionUtilProd.getConnection()
//              val useridToGocidHbaseTbl = connection.getTable(TableName.valueOf(dgn_map_userid_gocid_tbl))
//              val gocidToUseridHbaseTbl = connection.getTable(TableName.valueOf(dgn_map_gocid_userid_tbl))
//              val newRDD = partition.map(row => {
//                if (row != null && row.contains("event_type") && row.contains("ts") && row.contains("user_id")) {
//                  //                OPrinter.logAndPrint("For row: " + row)
//                  val gson = new Gson()
//                  val jsonObject: JsonObject = gson.fromJson(row, classOf[JsonObject])
//                  val event_name = jsonObject.get("event_type").getAsString
//                  val epoch_time = jsonObject.get("ts").getAsLong
//                  val user_id = Option(jsonObject.getAsJsonPrimitive("user_id"))
//                    .map(_.getAsString)
//                    .getOrElse("")
//                  val game_id = "Lucky Time Slots"
//                  if (user_id == "") {
//                    //Notifier.sendEMail("dgn_lts@octro.com", s"Data packet doesn't contain user_id for \n : $row", "yash.singhal@octrotalk.com", "Action required : Couldn't process dgn-lts data")
//                  }
//                  val rowkey = user_id.trim()
//                  var getUserData = (rowkey, useridToGocidHbaseTbl.get(new Get(Bytes.toBytes(rowkey))))
//                  //                OPrinter.logAndPrint(s"Get data from hbase table $ref_user_id")
//                  var gocid_from_hbase = ""
//                  try {
//                    gocid_from_hbase = Bytes.toString(getUserData._2.getValue(Bytes.toBytes("data"), Bytes.toBytes("goc_id"))).toString()
//                  } catch {
//                    case e: Exception => {
//                      //OPrinter.logAndPrint("Inside catch")
//                    }
//                  }
//                  var hbasePut = new Put(Bytes.toBytes(user_id))
//                  val event_time = Format.epochToDateTime(epoch_time)
//                  val colList = mapperEventCol(event_name)
//
//                  //            if(event_name.equalsIgnoreCase("level_up")){
//                  var eventValueJson = new JsonObject
//                  for (col <- colList) {
//                    //                  OPrinter.logAndPrint(s"$col: $colSchema.get(col)")
//                    val dataType = colSchema(col)
//                    val extractedValue: Any = dataType match {
//                      case "string" => {
//                        Option(jsonObject.get(col)) match {
//                          case Some(jsonElement) =>
//                            val value = jsonElement.getAsString
//                            eventValueJson.add(col, new JsonPrimitive(value))
//                          case None =>
//                            eventValueJson.add(col, JsonNull.INSTANCE)
//                        }
//                      }
//                      case "long" => {
//                        Option(jsonObject.get(col)) match {
//                          case Some(jsonElement) =>
//                            val value = jsonElement.getAsLong
//                            eventValueJson.add(col, new JsonPrimitive(value))
//                          case None =>
//                            eventValueJson.add(col, JsonNull.INSTANCE)
//                        }
//                      }
//                      case "boolean" => {
//                        Option(jsonObject.get(col)) match {
//                          case Some(jsonElement) =>
//                            val value = jsonElement.getAsBoolean
//                            eventValueJson.add(col, new JsonPrimitive(value))
//                          case None =>
//                            eventValueJson.add(col, JsonNull.INSTANCE)
//                        }
//                      }
//                      case "double" => {
//                        Option(jsonObject.get(col)) match {
//                          case Some(jsonElement) =>
//                            val value = jsonElement.getAsDouble
//                            eventValueJson.add(col, new JsonPrimitive(value))
//                          case None =>
//                            eventValueJson.add(col, JsonNull.INSTANCE)
//                        }
//                      }
//                      case "array" => {
//                        Option(jsonObject.get(col)) match {
//                          case Some(jsonElement) =>
//                            val value = jsonElement.getAsJsonObject
//                            eventValueJson.add(col, value)
//                          case None =>
//                            eventValueJson.add(col, JsonNull.INSTANCE)
//                        }
//                      }
//                      case _ => {
//                        //                      OPrinter.logAndPrint(s"for column $col else datatype: $dataType \n row $row")
//                        throw new IllegalArgumentException(s"Unsupported data type: $dataType")
//                      }
//                    }
//                  }
//                  eventValueJson.add("epoch_time", new JsonPrimitive(epoch_time))
//                  eventValueJson.add("event_time", new JsonPrimitive(event_time))
//                  eventValueJson.add("user_id_numeric", new JsonPrimitive(user_id))
//
//                  var goc_id = ""
//                  if (gocid_from_hbase != "") {
//                    //                  OPrinter.logAndPrint("Inside if line 251: " + gocid_from_hbase)
//                    goc_id = gocid_from_hbase
//                  }
//                  else {
//                    accum.add(1)
//                    OPrinter.logAndPrint(s"Fetching gocid for $user_id")
//                    val response = getGOCIDfromUSERID(user_id)
//                    val outerJson = stringToJsonConverter(response)
//                    val innerJson = stringToJsonConverter(outerJson.get("body").getAsString)
//                    goc_id = innerJson.get("body").getAsJsonObject.get("GOCID").getAsString
//                    if (goc_id != "") {
//                      hbasePut.addColumn("data".getBytes, "goc_id".getBytes, goc_id.toString().getBytes)
//                      useridToGocidHbaseTbl.put(hbasePut)
//                    }
//                    // Now putting user_id corresponding to this goc_id in dgn_map_gocid_userid_tbl
//                    var hbasePut2 = new Put(Bytes.toBytes(goc_id))
//                    hbasePut2.addColumn("data".getBytes, "numeric_id".getBytes, user_id.toString().getBytes)
//                    gocidToUseridHbaseTbl.put(hbasePut2)
//                  }
//                  val mainJson = new JsonObject
//                  mainJson.addProperty("game_id", game_id)
//                  mainJson.addProperty("event_time", event_time)
//                  mainJson.addProperty("event_name", event_name)
//                  mainJson.addProperty("user_id", goc_id)
//                  mainJson.add("event_value", eventValueJson)
//
//                  //OPrinter.logAndPrint("Value of no. of API hits for this hour: " + no_of_hits)
//                  (mainJson.toString, game_id, goc_id, event_time, event_name, eventValueJson.toString)
//                  //(event_name, event_time, game_id, hashed_user_id, eventValueJson.toString)
//                }
//                else {
//                  //Notifier.sendEMail("dgn_lts@octro.com", s"Data packet is missing some key for \n : $row", "yash.singhal@octrotalk.com", "Action required : Couldn't process dgn-lts data")
//                  //            OPrinter.logAndPrint("Else row" + row)
//                  ("", "", "", "", "", "")
//                }
//              })
//              newRDD
//            })
//            .persist()
//          OPrinter.logAndPrint(s"retRDD data count: ${retRDD.count()}")
//          var empty_gocidRDD = retRDD
//            .filter(row => {
//              val jsoStr = stringToJsonConverter(row._1)
//              jsoStr.has("user_id") && jsoStr.get("user_id").getAsString.isEmpty
//            })
//          if (!empty_gocidRDD.isEmpty()) {
//            val empty_gocid_count = empty_gocidRDD.count()
//            val sample_rec = empty_gocidRDD.take(5).mkString("\n")
//            OPrinter.logAndPrint(s"Didn't receive goc_id for these packets ${sample_rec}")
//            //Notifier.sendEMail("dgn_ovs@octro.com", s"Didn't receive goc_id for these no. of records ${empty_gocid_count} \n Sample records are: \n ${sample_rec}", "yash.singhal@octrotalk.com", "Action required : Couldn't get gocid for dgn-ovs data")
//            spark.sparkContext.stop()
//          }
//          var non_emptyRDD = retRDD.filter(row => {
//            val jsoStr = stringToJsonConverter(row._1)
//            jsoStr.has("user_id") && jsoStr.get("user_id").getAsString.nonEmpty
//          })
//          OPrinter.logAndPrint(s"Final data count: ${non_emptyRDD.count()}")
//
//          val lastReadDateHour = date + "/" + hour.toString
//
//          OPrinter.logAndPrint(s"Writing data on hdfs for $lastReadDateHour")
//
//          //non_emptyRDD.map(x => x._1).saveAsTextFile(s"/dgn/lts/kafka_events_new/$lastReadDateHour")
//          OPrinter.logAndPrint(s"Data saved on hdfs successfully")
//          val outputPath = new Path(s"/dgn/lts/kafka_events_new/$lastReadDateHour")
//          val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
//          val appendStream = if (fs.exists(outputPath)) {
//            fs.append(outputPath) // Append to existing file
//          } else {
//            fs.create(outputPath) // Create a new file if it doesn't exist
//          }
//          val writer = new OutputStreamWriter(appendStream)
//          non_emptyRDD.map(_._1).collect().foreach(line => writer.write(line + "\n"))
//          //commenting the kafka code -------->by yash
//          //OPrinter.logAndPrint(s"Pushing $hour hour data to kafka topic $dgnLtsTopic")
//          //OKafkaProducer.pushToTopic(dgnLtsTopic, non_emptyRDD.map(x => x._1))
//          //OPrinter.logAndPrint(s"Data saved to kafka topic $dgnLtsTopic")
//          retRDD.unpersist()
//          OPrinter.logAndPrint(s"Value of no. of API hits for $hour: ${accum.value}")
          //put in the hbase of processing
          val put = new Put(Bytes.toBytes(date))
          val columnFamily = "hour_data"
          val columnQualifier = f"$hour%02d"
          println("Reached to write the data in the hbase")
          val allPathsList = dateMap.get(date)
            .flatMap(_.lift(hour))
            .map(_.collect { case (path, _) => path })
            .getOrElse(Nil)
          //println(allPathsList)

          if (allPathsList.nonEmpty) {
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnQualifier), Bytes.toBytes(allPathsList.mkString(",")))
            table.put(put)
            OPrinter.logAndPrint("Data Write Successfully in the Hbase")
          }
          else {
            OPrinter.logAndPrint(s"No data is available for the ${date}/${hour} in the HDFS")
          }
        }
        OPrinter.logAndPrint(s"Value of no. of API hits for : ${accum.value}")
      }
    }
    catch {
      case e: Exception => {
        OPrinter.logAndPrint("************************************************************************************************************************************")
        OPrinter.logAndPrint("Spark Context Stopped")
        OPrinter.logAndPrint("************************************************************************************************************************************")
        spark.sparkContext.stop()
        //          val stackTrace = "com.octro.start.main.StartMain :: " + ExceptionUtils.getStackTrace(e) + "<br/><br/>"
        //          Notifier.sendEMail("dgn_ovs@octro.com", s"Error occurred in dgn_ovs connector for date $start_date hour $hour " + "\n<br/>" + e.getMessage + "\n<br/>stackTrace = " + stackTrace, "simran.maurya@octrotalk.com", "Action required : Couldn't process dgn-ovs data")
        //Notifier.sendEMail("dgn_lts@octro.com", s"Error occurred in dgn_lts connector" + e.getMessage, "yash.singhal@octrotalk.com", "Action required : Couldn't process dgn-lts data")
        OPrinter.logAndPrint(e.getMessage)
      }
    }
    }
  def dirExists(hdfsDirectory: String): Boolean = {
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val fs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    fs.exists(new org.apache.hadoop.fs.Path(hdfsDirectory))
  }
  def getGOCIDfromUSERID(userId: String): String = {
    val gocidEndPoint = ""
    //    val gocidEndPoint = "http://serv117.octro.net:4004/userprovision/guestReg" //"http://serv117.octro.com:4004/userprovision/guestReg"
    val action = "guestRegister"
    val data_from = "dwbsid"
    val guestId = userId
    val appid = "20240221mrpi" // LTS app_id
    val connectionTimeout = 10000 // 10 seconds
    val readTimeout = 20000
    try {
      val response = Http(gocidEndPoint)
        .header("content-type", "application/json")
        .postData(s"""{"action":"guestRegister","appid":"20240221mrpi","guestId":"$guestId","data_from":"dwbsid"}""")
        .timeout(connectionTimeout, readTimeout)
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
        OPrinter.logAndPrint("Inside Catch Block as {")
        val stackTrace = "com.octro.start.main.StartMain :: " + ExceptionUtils.getStackTrace(e) + "<br/><br/>"
        OPrinter.logAndPrint(s"Inside Catch Block as ${e}")
        //Notifier.sendEMail("dgn_lts@octro.com", s"Error occurred while fetching gocid for userid $guestId  " + "\n<br/>" + e.getMessage + "\n<br/>stackTrace = " + stackTrace, "shreyansh.maurya@octrotalk.com,govind.gopal@octrotalk.com,akashdeep.singh@octrotalk.com", "Action required : Couldn't fetch gocid from api")
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
  def stringToJsonConverter(json: String): JsonObject = {
    val jsonParser = new JsonParser()
    val json_element = jsonParser.parse(json)
    val json_data = json_element.getAsJsonObject
    json_data
  }
}