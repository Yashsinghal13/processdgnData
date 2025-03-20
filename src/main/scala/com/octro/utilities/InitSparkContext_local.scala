package com.octro.utilities

import org.apache.spark.sql.SparkSession
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object InitSparkContext_local {

  import org.apache.logging.log4j.core.config.Configurator
  import org.apache.logging.log4j.{Level, LogManager}

  // Local

  case class CommandLine(application: String, prodOrElse: String, iterationParamFile: String, startTS: String, endTS: String)

  case class Oconf(sc: SparkContext, spark: SparkSession, hashp: HashPartitioner, conf: SparkConf, jobType: String, scanStartDate: String)

  def getSparkContext(confParams: Map[String, String]): Oconf = {
    import org.apache.spark.sql.SQLContext

    //    LogManager.getRootLogger.setLevel(Level.ERROR)
    //    Logger.getLogger("org.apache.spark.SparkContext").setLevel(Level.ERROR)

    val log = LogManager.getLogger("org.apache.spark.SparkContext")
    Configurator.setRootLevel(Level.ERROR)

    //Initializing spark
    val conf = new SparkConf()
      .set("spark.streaming.backpressure.enabled", "true")
      .set("spark.streaming.kafka.maxRatePerPartition", "50") //100000 //50000 //25000  *60 (duration)
      .set("com.couchbase.nodes", "serv98.octro.net:8091") //For CouchBase
      .set("spark.ui.port", "4545")
      .set("spark.executor.memoryOverhead", "100m")
      .set("spark.hadoop.fs.defaultFS", "hdfs://octropc")
      //      .set("com.couchbase.bucket.teenpatti_test", "sonic12345")
      .set("com.couchbase.bucket.teenpatti", "sonic12345")


    var jobName: String = ""
    var scanStartDate: String = ""
    //      var spark = SparkSession.builder()
    //        .appName("Poker Events LTV-Daily Profilers")
    //        .config(conf)
    //        .getOrCreate()


    //overwrites or add few parameter(s) to spark context
    if (confParams.size > 0) {
      confParams.map(params => {
        conf.set(params._1, params._2)
      })
    }
    //      .set("spark.locality.wait", "100")
    //      .set("spark.executor.extraJavaOptions","-XX:+UseConcMarkSweepGC")
    //      .registerKryoClasses(Array( classOf[OStreamer] ))
    //      .set("spark.streaming.kafka.consumer.poll.ms", "512")
    //      .set("spark.streaming.stopGracefullyOnShutdown","true")
    //      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    //      .set("spark.driver.extraJavaOptions","-XX:+UseG1GC")
    //      .set("spark.streaming.concurrentJobs","4")

    val sc = new SparkContext(conf)

    sc.hadoopConfiguration.set("dfs.nameservices", "octropc")
    sc.hadoopConfiguration.set("dfs.client.failover.proxy.provider.octropc", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")
    sc.hadoopConfiguration.set("dfs.ha.namenodes.octropc", "hmaster,hmaster22,hmaster24")
    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.octropc.hmaster", "hmaster:9000")
    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.octropc.hmaster22", "hmaster22:9000")
    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.octropc.hmaster24", "hmaster24:9000")

    val spark = new SQLContext(sc).sparkSession

    val oConf = Oconf(sc, spark, new HashPartitioner(60), conf, jobName, scanStartDate)

    oConf

  }

  //  def getSparkContext(): SparkContext = {
  //    this.getSparkContext(Map[String,String]())
  //  }

}
