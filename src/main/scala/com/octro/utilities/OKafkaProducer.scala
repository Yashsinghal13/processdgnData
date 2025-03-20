package com.octro.utilities
import java.util.Properties
import org.apache.kafka.clients.producer._
import org.apache.spark.rdd.RDD

object OKafkaProducer {

  def pushToTopic(topic: String, data: RDD[String]) {

    val props = new Properties()
    //    props.put("bootstrap.servers", "192.168.111.51:9092")//"192.168.123.6:9092,192.168.123.5:9092") //192.168.123.8:9092
//    props.put("bootstrap.servers", "192.168.123.5:9092,192.168.123.6:9092,192.168.123.8:9092")//"192.168.123.6:9092,192.168.123.5:9092") //192.168.123.8:9092
    //		props.put("bootstrap.servers", "192.168.123.5:9092,192.168.123.6:9092,192.168.123.8:9092")
//    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
//    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")


    //		val TOPIC = "test_hadoop" //test_hadoop
    //		val part = new Partition("",1,)
    data.foreachPartition((partitions: Iterator[String]) => {
      val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)
      partitions.foreach((msg: String) => {
//        try {
          producer.send(new ProducerRecord[String, String](topic, msg))
          //          producer.send(new ProducerRecord[String, String](topic,partition,key, msg))
//        } catch {
//          case ex: Exception => {
//            OPrinter.logAndPrint("Exception in pushing record to Kafka")
//            ex.printStackTrace()
//          }
//        }
      })
      producer.close()
    })

    /*
      data.map(msg => {
        val record = new ProducerRecord(topic, "msg", msg)
        producer.send(record)
      })
      *
      */

  }

  def pushToTopic(topic: String, msg: String) {

    val props = new Properties()
    //		props.put("bootstrap.servers", "192.168.123.5:9092,192.168.123.6:9092,192.168.123.8:9092,192.168.123.207:2181,192.168.123.208:2181") //192.168.123.5:9092
    //    props.put("bootstrap.servers", "192.168.111.51:9092")//"192.168.123.6:9092,192.168.123.5:9092") //192.168.123.8:9092
//    props.put("bootstrap.servers", "192.168.123.5:9092,192.168.123.6:9092")//"192.168.123.6:9092,192.168.123.5:9092") //192.168.123.8:9092
    //	  props.put("bootstrap.servers", "192.168.123.5:9092,192.168.123.6:9092,192.168.123.8:9092")
//    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
//    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")


    //		val TOPIC = "test_hadoop" //test_hadoop

    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)
    try {
      producer.send(new ProducerRecord[String, String](topic, msg))
    } catch {
      case ex: Exception => {
        OPrinter.logAndPrint("Exception in pushing record to Kafka")
        ex.printStackTrace()
      }
    }
    producer.close()

    /*
      data.map(msg => {
        val record = new ProducerRecord(topic, "msg", msg)
        producer.send(record)
      })
      *
      */

  }


}