//package com.octro.start.main
//
//import com.google.gson.{Gson, JsonObject, JsonParser, JsonPrimitive}
//import com.typesafe.config.ConfigFactory
//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.fs.{FileSystem, Path}
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.functions._
//import org.apache.spark.{SparkConf, SparkContext, sql}
//import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode, SparkSession, functions}
//import org.joda.time.{DateTime, DateTimeZone, Days}
//import scalaj.http.Http
//import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
//import org.apache.hadoop.hbase.util.Bytes
//import com.octro.utilities.{Format, Notifier, OPrinter}
//import com.octro.utilities.HbaseConnectionUtilProd
//import com.octro.utilities.OKafkaProducer
//import org.joda.time.format.DateTimeFormat
//
//import scala.collection.mutable.ListBuffer
////import com.octro.utils.OKafkaProducer
//import org.apache.commons.lang.exception.ExceptionUtils
//import com.google.gson.JsonNull
//import org.apache.hadoop.hbase.TableName
//import org.apache.hadoop.hbase.client.{Append, Get, HTable, Increment, Put, Result, Scan, Table}
//import java.security.MessageDigest
//
//
//object OVS_correction {
//  val dgnOvsTopic = "dgn_events" //"DGN-Events"
//  val dgn_map_userid_gocid_tbl = "dgn_ovs_numid_gocid"
//  val dgn_map_gocid_userid_tbl = "dgn_ovs_gocid_hashedid"
//  val dgn_map_hashedid_gocid_tbl = "dgn_ovs_hashedid_gocid"
//
//  def main(args: Array[String]): Unit = {
//    OPrinter.logAndPrint("*************** Welcome to DGN Connector On 29th Mar'24  ***************")
//
//
//    val conf = new SparkConf()
//      .set("spark.streaming.backpressure.enabled", "true")
//      .set("spark.streaming.kafka.maxRatePerPartition", "50") //100000 //50000 //25000  *60 (duration)
//      .set("com.couchbase.nodes", "serv98.octro.net:8091") //For CouchBase
//      .set("spark.ui.port", "4545")
//      .set("spark.executor.memoryOverhead", "100m")
//      .set("spark.hadoop.fs.defaultFS", "hdfs://octropc")
//
//    val sc: SparkContext = new SparkContext(conf)
//    val spark = new SQLContext(sc).sparkSession
//    println("Application id=" + sc.applicationId)
//    import spark.implicits._
//
//    var numPartitions = 10
//    try {
//      val numExecutors = sc.getConf.getOption("spark.executor.instances").get
//      val numCores = sc.getConf.getOption("spark.executor.cores").get
//      numPartitions = numExecutors.toInt * numCores.toInt
//    }
//    catch {
//      case e: Exception => {
//        e.printStackTrace()
//        numPartitions = 10
//      }
//    }
//    val userList= List("6517512d-547a-4330-a2fa-47151494121a","6efb2a3c-2572-4d0a-a7e3-e0dcf8ce6ecc","0cd3ec2f-4762-4778-84bd-5f1e78a7b6fa","75e38546-862b-4ab0-a1f7-e20a36b3542e","d0a0a05a-343a-4cc9-8137-1792b1e5cd5b","ee87e4a3-b9e8-4612-a0d1-e1958e8cdc81","45fec7da-2aa7-4c17-b7a5-d9c197e072de","59ddd7f2-a67f-4dbf-bffc-84d49b53d167","4c635167-4eaa-4064-a1dc-bb29cf931480","1c7b870f-a453-4d55-8193-2e734fd8074b","5ec57d51-2804-4acb-8e74-0c7ac2f36c63","8b55c2ca-e054-4c2e-9989-047091c7fb91","9bfdf372-61db-449f-ac4b-eb303b35cb7d","8b8e68db-39d2-42eb-9c1a-73639bcdbf0f","af6123de-4c25-4222-94e5-359be6aa5123","52ef27e3-95ef-4e70-bf55-540efd357c43","53a0701c-01b5-4d98-9e1e-4572eab12029","5bb56b70-7494-4333-aa6e-fdc921f89059","653318b3-21f6-4456-ae45-822c543949d8","8a3a0789-75ac-4f7d-896f-633afd6331fd","223bdf8f-d00a-4b4b-bce4-ed26594d926c","a72679ad-11cf-4d66-abf6-81301567c6cc","195c72c3-52c1-4bca-bdc5-c881a397a5d0","37473c72-f1ba-43d5-8bb8-63b1b96fd399","e11ae956-97b8-47bb-8425-6a00e124fd53","7cff363b-274d-4e5b-ae82-2220ed2195ed","34c22622-2131-4222-8d42-1561f04a3e9e","eb901178-9c95-425c-8a52-c4ad1a4778a1","7b9bb9c0-4875-434e-a3f0-9735440eb722","fd5eff0a-37b6-4c7a-8bc1-aca0fd10b188","c72751de-57a5-4f7c-ac4a-d9cf31582951","82ac9c68-c87b-4870-a3a3-bcb4f907850e","9ca66688-cdb0-4b90-bd0f-ba1800c57775","fc0ccfb3-9c6d-4cd3-b027-1e0d0f095f07","421a3740-a1eb-4b84-a4b3-a38a1804f92b","90dc9078-f577-48bd-bcad-f0ce35bca349","709e5192-538b-432a-9c41-3a65e2bbcd92","35b13754-1366-462f-851f-d3fe5c79f3d9","e93f707b-6944-4bf4-a1ed-a83cc4274c69","ffdbe30e-a1a8-4ec8-8f9e-0d8454103314","d86ad76c-a608-4dee-87f7-7a4414e57c68","1c55a618-2c4d-496e-9b87-dc8e404d201c","cdc3e310-1aa8-43ad-a391-b922c8873bb9","ff8fb89c-390e-481c-869b-226fdb0d5ad3","1f4a4a1a-536d-45af-9f12-405cb9b873e6","e745d193-8650-4c5f-abf0-af842ef3c56c","7709af0e-8e6a-4ba8-ba23-728a62a90f5b","019dafb8-ac48-4795-9b7e-2ad3ec4d0923","5c56d718-6591-459d-8ddd-1c62388325f7","8c7a0f9c-fcd4-4600-896a-aef06f98505f","8db3e7c1-75d6-4b01-a1cc-76e42939da82","b945d05b-5466-4c0d-967a-22a863b1974c","5dfbb144-2232-4098-9b22-0c19875b6638","2e296abd-79c3-42ec-ad96-4d4db4ae0ab0","a844c672-3159-4cc2-9984-41ba6f860a71","0f8519e2-30ad-45dd-ae73-879af27e2533","03faa160-ea9e-4e96-87e5-a6faa15b2ef3","82c60ea7-9d69-42fe-966d-f31f64e218b7","85cf65e3-be6f-48a9-8778-4442ab42b843","d6556a9b-ff01-41ea-b555-e48daddaa9d3","f4be6a3d-2b5c-4357-b925-7ae075ae89cd","0facf01c-28d9-4782-b290-900b3eb23494","636980f0-a5ae-4412-a8e5-036782effd9b","24d6fdbc-62e8-438e-a2f7-6eebe76fb738","ca94dfec-922c-4ff4-8f53-9ab29235ae17","d0b734df-446c-4ba6-a1c8-395915c15c21","ba30eaa7-bec2-49a8-af68-9e6c0cb4db86","4a4f8e6d-df60-4440-bcfa-c8d9085f8bfe","ed8f5b0b-0162-4ea9-aa42-b3598da8c4d4","55051a07-545e-4cac-8298-33e579b2a9fc","24bfed05-c0b1-497b-985c-aa033179cb70","6a05628a-006c-4e88-a6e1-80474eedead4","1f773d8c-c52e-4f17-9687-287692c67472","c47d7c47-3e40-4368-a85a-62049ae580fe","4b534799-0157-455e-8d8e-c2f45d21d66b","8c8e35b5-9ac1-414d-b85e-e3d38e6a746d","e22de55b-f387-40f7-84cc-7c2bd4c5d7de","7efae417-a705-48ea-810c-e608c2044873","0f41f28d-c9d3-4a1c-9d5d-8a8483d403f4","3d936bc3-cce0-4c5e-8be4-f63d00043a58","123abab9-60df-4766-98ab-15b40641aaed","f2365b08-42d1-4324-9d6d-ec425934682a","077bbfef-7c2d-4c70-a095-3814b1e15d10","5d8cb0e8-2d90-48a8-bb77-79bea0d330e2","f02a92cb-2392-4ebb-a084-512bfb995173","42cf1fe5-0451-4ab8-a382-459cbaa6a5e2","99bc35e5-a110-4fd2-84cf-24bda9a05d2c","5c364fa3-1919-4c15-85c1-3f789c5c59e0","1b31e372-f35e-4e7f-b61f-3313ff53beac","03872111-22b9-4a98-bf1b-950cdd6a95e1","cd89c0dc-bb9c-41fa-8b77-883ea7a81214","c0fd4cc0-e1cf-4348-a7fa-74c6a3dde7bd","ca813f43-dd6a-493b-bdd7-57d5bbd53802","30058d81-6ba5-4916-beb2-0f6ba027213e","71b39f06-7393-4b46-959a-f9e2c3b826ce","b148b45f-5797-4d33-9819-8738a6c83e3c","1c64ec0d-c942-4ff5-acce-ae4228877f43","f2d1325a-e5b6-4dcf-82fa-2d4f75dba132","db9a5d94-2977-4c3b-b7e8-17dc310c6e57","ea84a620-04a0-4880-a328-c8fdfda2d2c7","341a2a47-7689-4c02-9bdb-b13419f8e3c4","c1535044-2795-436e-ab1b-0e913b79d287","fc95bd9d-83b4-40a6-b758-092f8f6a4064","7f7373ec-0ba7-4db6-9295-cce6773a935f","e2d28cc0-aa05-4f57-be90-6814d045304e","cdc63192-2e68-40c6-84e7-3eaa8d03fdf0","0073b7ee-8b1f-401e-9e91-82bbd7175707","233e4f14-e2fb-4a20-80b9-05aa8f476e94","ea74937d-5b3b-46b3-a970-f511296ad1c2","0dd51626-b5a5-42db-8fc8-94d217cafba1","69fab475-0a27-4313-8189-7411f8f549e6","7ff2643c-7a1d-4506-acbe-a64cace3892d","95d4436b-c1a3-4129-b5d0-1c862fe5918f","5f48b2bc-206d-46c6-8aaa-116739c5fb57","a85f48ba-c2af-4207-b0e4-f45e44962493","4b6ae0fa-edb2-424f-b83a-ccfbd6a0880e","494b86ef-4a7a-44df-ad14-29eb0d6fa6a6","ba6df4af-ddea-4371-bb88-8ce51a6170d7","3f5476c6-8201-4c82-a19c-98dc807641dd","1606e8de-9c43-4ea8-9c4d-2510d0abc97c","1e437888-685b-4447-91da-1ada76456f15","adf23284-d760-4de5-90f0-be9360b9156e","2f8facc2-296c-4d75-bfb5-efbb5cbd007f","e022758c-3f90-42f5-aa68-64cc9ff21251","e094215b-6a86-4eca-9250-7ca1feaff7fb","401b43fb-022c-440c-b0ae-d93af840770c","6dc45e3d-1933-4cd5-9f8c-d7b6bdd80dd9","acb79817-30ca-4b23-86b4-46463e7e9df6","268de20e-19b0-4b6b-a985-2a30e1624c76","6d228ecc-aa1f-4c2b-ad90-a6b7c5531414","033f6fe9-87e9-455e-b898-d04aa927c783","500447b7-f6f0-49e6-a86d-f97a2b6ac59e","ed17fa4b-730c-408b-baed-3eef69db7cda","96309947-9622-4aed-8011-aab163c79aa9","e5242bde-f3b8-41d7-a33d-b072faa8f499","0fc30670-80a7-4105-9d5f-0c7b373c00e7","b520b854-6a02-4337-a6f0-9cc79c2177a4","6ec7165d-2745-4ec0-bea6-ebd020f8a4d5","e1bf09e8-52c5-46cc-b768-06c845d498f3","93a3ee9c-1b85-4951-888f-a1fba41d3651","96ca8dd2-d192-4b67-8fb1-afbb5886699e","d6d88588-00b6-4997-b515-983ebff9c45b","a5fbbbb5-8e89-4c9a-874f-b0a6f75d8086","7743afe5-333c-4341-a19c-dacd3345b4f0","06db9122-9217-4b1a-b215-1e4d923eba58","67641919-27c3-4da5-a883-e114f42f9cd3","73ba84b3-8ffb-40d8-a508-0d5325d12178","415a52f3-2eb5-4761-8e72-7ae171dc9d00","840ddc6f-bb44-429e-a255-9c65af74aa17","0923e425-2049-42ea-859d-1630a53b659e","d5a393ce-cad9-4b97-a616-f5e700de1834","da69318d-3a8d-4f17-9f3c-1e521dc411a6","78c2b444-bef5-4101-b045-bd9be53b2540","9a7c482a-487f-4e47-9aef-c2fa37ea5f43","7c34e1c4-73ba-4eeb-8528-880230c7804d","cace59d4-e346-4b2c-a724-84f0272a8a42","cae60212-81fb-4078-ac61-521c4c4b9cd7","5a8e09a2-b533-4a16-b621-75c5ee65d1a2","14a93a08-7c7e-4aa3-8fe9-a14a2cef7cdc","50a7f6fe-5c78-464e-adb3-6805af6ed678","e97fbd29-a5d3-4c08-ba17-5d74d544ee4f","fbfd0051-3009-4117-b2b1-6cdebaab19c7","88ef3a90-e896-4385-8975-7fcdffd3398f","c53ae9ad-8532-457d-8b61-2110042d15f3","333151f6-b714-4a42-8711-9231f5d3d170","9cdc5498-c327-4cbb-90a4-9e09070ac5da","7d892404-270f-4993-aa7c-14cb736bf97c","29963eb8-eabb-42c2-a7d3-2a2ee6749a41","fcfe62c5-f1ba-443b-97f0-e8ab2c5cbac9","6f5b8710-ee4b-4030-8a8a-c0ab75d4f601","215ecd12-cde2-4031-ac6d-e1dd1568430e","2d342511-f31c-442f-bed3-1306746e598f","af9d9d24-b936-46c0-a754-99df11be3d03","dabca6ba-fb15-418d-b32c-59a594797df3","55249afc-1622-4ab9-8735-bc1ce3501b00","81029067-8c20-4cbb-b277-3c17f03ee2e3","4ec8acb6-7c55-4c92-b330-9c991581f36f","6b695e07-44ce-44fa-9f10-4d8d1c24ac0d","bb335fbf-2a26-48e0-a3dc-38ae1c1f23a7","54d37de1-239c-470d-bbd6-812a27b90137","98e7563b-c26b-4da1-863c-0e47d4f32f5c","7a95c2af-c1f5-4e18-b370-bda6706fd43d","c934d645-19d2-4cf1-a2da-d92019d7b7c0","b09e6d95-0b39-4fa3-8e51-560eb45daba8","537eaea0-0c3e-4278-976d-b5c08278ad6d","67023a20-8f56-4878-9e33-6137d1286cfc","0fa85ca9-cb6f-4cfc-8cd8-90d2345d1ceb","d9e0a4ef-5bc7-4a3c-8541-fe8c6e11ea92","e8c25646-91e5-4ae4-aa11-3b19a67e873e","8e669ffb-690b-4bad-9879-142d5ab6f2d1","b6161637-ced9-4d06-bb1f-4c61a8b1832a","e1a39e6a-6e71-4518-940c-f9b95394defe","4f7cec7f-aa6a-46b7-a1c3-5fa6a7e841fc","1dcf0cdb-716a-40b9-97aa-3c613a9937be","d9a9430a-db55-4aba-8fb1-107bbf39bebd","50fb5a56-2e0a-4f61-af22-1be97511ab29","53f2e5a2-c5c0-4d9d-a2f9-fdc2ebdfaa79","496c05d6-9c29-4375-b59c-3f52b30639ad","de334897-d05b-44df-8a79-93776ee430bb","1eae02c6-071d-4336-8489-8ccd71143457","21d685a9-90f6-4c55-b266-61cf35f56d8d","0937dfd2-e039-4aef-8257-62627e254461","94f79d79-da02-443d-80ad-6db594689c92","214f0308-b320-4d7e-8339-dbe2393448e1","4cb6885e-6a11-4c62-9e69-0e3b128008d0","7f9c2e26-ae7b-42c0-be38-e67f1f689d91","6eae8c21-cc19-48c0-9c0c-12e0c23f161f","cf3753a7-f917-4b72-8e47-ebd33217c907","814e3661-3991-4316-b6a6-501cac0fd690","17835bc9-0253-41f9-afb6-bffab5aef992","6f39e408-de9b-4b00-8052-946c475d9db7","7ee25ac5-369e-4c63-b90a-31f89134204f","8cdcc893-39dd-46d5-ab33-fcfe84715282","86793210-bf50-4ab6-b565-d5875afeba64","03b5dbe4-4fae-42b5-87a7-12847df0d668","ce2cf9ac-3e91-483e-a999-24e948bcf11c","9a1e994f-c6b4-4b2b-8a17-b8261faf3857","c2d7f8bb-c6cd-43a7-943e-9844c66e537d","c387f025-819b-4153-8640-81722eb8c785","0ca798f0-c1a6-4e34-bdb1-f95c41acb0b8","e8d4d970-ad00-4b9b-b002-67992cadc8fb","5043cee4-7af4-49d8-8d2a-d337bb3f2d4f","59a1f83a-5a67-4456-9904-fdc2697145ac","2aa1f074-1a22-4610-83d4-d165ea23dbac","a55bbc4c-aeb7-4d4f-9708-24611f3b3cf8","071393a9-c6a6-4dd3-b55f-6d0c56379f7b","072be93c-fc57-45f0-bcdf-5b3cfa896ee1","7f90cf6c-e1f3-4bab-b43d-d94faf50c4f9","5a07f481-f642-4b94-b123-53bca76d0b1e","a3dcf345-27b4-4d1e-bd6c-b013542d432d","db858dae-8afa-481e-bab3-b2337b684dbf","ea7b27dc-ae83-4de0-8b61-2ea54053eab0","706896d2-842d-47ee-a6fb-c2969a4679c4","e17a887b-ff53-45fd-8467-d3d37f193b5b","57f7a173-70dd-449f-a731-5183b2675a11","39346d74-eebe-42b2-929c-ff6bcf814993","62fbea9a-9813-4b1f-a8f6-e60b5ccbde21","7c53466b-c97d-4cda-9852-9cf30542d478","bff87b4b-0918-41b9-8507-5f54fdb5cf0d","acd0236c-f4dd-4235-b499-7216a72f48c2","99262954-75d8-4431-81af-f1b08e323bc2","2471eba6-a136-4ea2-bc47-394fc717da57","7b6ba9f1-27b5-47d7-83c0-b830c9940f97","0d81f64b-e192-4f0c-b695-a29ed24ba23b","7920b3a2-9041-4851-af95-4ef01eae6c0d","d0075aae-9f56-4b21-89ef-8f1332588033","3522f13c-0c84-4c89-b59c-0bad751fd8d9","64f7666d-fdc7-483c-b4dc-7b185f66db36","ad169280-617f-400d-bd2c-e05bcd653c14","3874bed5-1584-4ee3-a476-6f889ec62486","0ad55bc8-157a-431b-92ed-f444869247b0","1068793b-65f8-417d-85ba-89be6ab4dc40","20cfe771-62ec-4431-8df4-d6dade65d783","953d827f-eaaf-40b2-af1a-e4091848477a","358c8442-5e15-44d6-b8c5-18a74ba39f06","d9396e3b-3109-4c5f-952b-07a42a7f8148","54126d87-0e64-4010-9252-1d234b27899e","cf6f5a48-565b-4367-8852-00030643c824","df927a69-b2eb-46bf-b8d6-c8285611fa58","54b6e942-d508-44d5-b94c-820cb96201cd","ab7a8274-bd16-4007-9f46-8d82b85e0311","00b92a32-11e3-4d49-ad78-7018da32eeac","97d4244a-4ffb-4b01-b2b1-e30f30f56269","001e2b31-35cf-4a9d-8051-7a9b5b96ae8c","b09a2203-5060-4002-85d3-ed6553b0c01a","b5fb0afd-66a7-44d9-950c-bf998e0521e4","34790f39-9db6-469c-aecb-671d9427b739","e4ec83b8-a2d4-4a97-8d75-34bdf544c879","b0ce0e10-418a-4318-9cc7-28770c2daab1","32411f53-88ca-4f20-afad-e9913d8505a3","a17c2d37-a661-4329-9057-c12a015fce2e","f35afdae-1e3e-4332-8ffe-454f3fe6a513","e39d4a1c-1aed-45ec-9c30-89e7e3cc3a24","0e5b138f-84ce-40e1-aa07-0725d7173c79","a0472cb6-cf73-4b95-af88-b6a648bbff50","f2826479-46b4-4cdd-9fce-db9a45b8d38f","429b17a2-3f30-47f4-b36c-7e70ee01f801","8b37e608-d3e6-44b7-953d-7af17753c3cd","c9122983-19c7-4fc2-8c18-874b8c02e479","05c48430-e6dd-4af1-baf7-e7e88624957e","79aba67e-ca4a-49dd-a92e-4ee3d417a17c","03063419-7591-4300-b599-c21588c544b4","de1f0e27-b2dc-47e9-9975-d57fd8c63990","2e71d253-bb26-47f1-9a1a-ef44d6a1a676","c5c01533-5a17-4fc4-8119-65b67eab5eda","4a254d4e-c196-4552-87d3-ec7c2463cbc1","066d5465-12cf-4b1f-b8a6-effff7e62424","028e9a61-0c7b-4c76-b5c1-af7201092f09","3e7e844c-0d74-45dc-b010-2e6c76cabac6","74449c16-5139-4cd9-accf-cdbbbd6572db","9a89c6e6-8c89-462b-8795-41e3e1144df9","ae637eee-607e-44f4-8073-016fb4c26342","6cf5226d-a446-41fe-9fa4-35d07634f100","4cbe7928-8fa6-4d6a-bc7d-6094b2812ece","85300366-d5f8-4d9b-9890-510c21d82e11","ec2394c7-5c04-4c42-b150-7f44546e5120","45e385db-f224-426f-a7a4-a612e250d5e7","73e23a1d-b6da-4ada-9944-93a37657ab9d","489b89d1-ab1e-4245-be1d-7cc79bc58c69","5989636c-a4de-440c-a7bd-397f44f54918","6802c01a-9ad8-4f94-b157-2fbd32056675","c731fb49-a341-4505-a25b-9d45ac7b5b7b","9475d72e-2ffb-4f47-9c04-d08ad9a35768","c0c9a74f-1a57-4dfc-9e76-4ec789cf9aa9","d8d2396a-5395-4882-9763-de9ab298b17c","0aba517e-35e6-4e88-bd48-7231dbb41c9f","d61e4e49-b2bc-49ed-98b7-27f9592316b1","b7228e0d-0304-4f3e-8254-2d0dc7854932","bf824f2c-b397-414b-b293-6978b9088fcc","58b270de-73e0-4cc7-bce7-1460792825e3","899b9d9d-667b-4fff-91cc-517552f5fad4","4654c73a-ef97-4d20-89c8-ea50e0c4040c","2409928c-24fb-42e2-9f99-f1b991ab0c8f","50f85a17-c902-4e53-af46-807ad2fa0465","31f4ed29-641b-4c09-83ac-5eb05edeec0c","43658042-a297-4e3f-80b6-b1c647fd7be7","784ec418-8afc-4122-8896-f946e8e62e23","1a252018-0efb-4a01-b83a-e898d89ff632","a17d669f-eba1-45f5-ae85-c25213a99946","3a6ef226-70b3-405a-b947-6c13a179c6ec","1f84d4c2-7d7c-4206-95c8-f17be895cc2f","1983c2d4-7667-447a-beaa-5b9913c497ec","d80068f5-9b45-49ce-b1ba-bfd3ad0366e2","1ec589b9-0b06-4b3b-874a-c10b131216de","2f228d1c-3981-4ab1-9a37-57f18f229004","369aa98a-a543-4264-9d71-671f681a64c1","9e81ef5b-f7cc-4c83-a2e3-f5ec969e8ddd","0a51469f-228e-45c2-9199-b9524f2e6036","864ff88e-5dc8-44a4-8ed3-3b1c0835c950","b1491e62-1531-427f-b13e-c10790a7d82a","e9ef821d-245c-4d75-99b2-64d7c9a4371b","83f665b0-c19c-42a3-b974-a3db1dcf93cb","f4bb9d2e-e3d6-4e5c-a01e-7f950ae2952e","a3e3c2fd-e1f5-4760-a8a5-ba3b29cd9989","d2db7146-6b9b-4774-822e-ff768dfe5340","24a5a20d-1aa0-4735-ac9e-0ca73ae203e9","7bdaf8c7-b50e-4d20-b4f4-74e21c96e88c","43aafceb-8cdc-4b20-a0e2-b960408605b6","a0cabf05-16a8-434d-8928-a998b33e7434","b03373ba-f37b-46ec-bec5-477f4fa19252","4545fd2a-641a-4cd7-b9a1-df68b1660433","3524a13f-14d7-48fe-ad4d-9545672f2905","949253eb-eb9d-4ace-acfe-ddacd8bac39c","1d66bb84-0b1f-49a0-aa55-13cc53367339","dfc42253-8716-485a-8842-f8c04cc12f71","aa71e904-2d09-49f6-8788-a9445eb90038","9ed771a8-424d-4276-bfdb-c1c3d0b1b79a","43215494-337b-4aef-9c5b-7b6e298f7856","ccc75264-7f5a-4f53-b4da-ac46389cd30a","cb880d8b-cdef-48af-83b3-a5013b20f9ea","465b3e3d-38ed-49a0-8b8a-e4d146381ed5","42f4a9a1-6c25-4819-99b7-81ba3f901a79","54c779d6-0009-4c02-8491-904b9f2c1986","571cc6fc-06bd-418d-977e-6fbe292a95f2","072a612d-716a-4277-af0a-bb94adbd134a","662f116d-fa8a-4421-974d-65eaa93387b8","133534e8-4467-42db-8376-84878c354975","21aba9ea-16af-4da8-8e45-8f085957b5fb","db5737a0-4566-4d86-b21c-3ab6371c918d","0a32b1be-316f-4610-b0d7-b15c0b408f25","3d26d286-d741-4756-81b3-eda33b4b0314","67995e49-bb30-4df7-b203-6316945903fe","1b91f0aa-9023-44ee-80b1-f52ffa142c8d","a7193cc6-6a32-40ab-a1c6-32a719ec5c3b","0164e01c-1720-4c76-89e4-0f0806236421","0988385e-b28b-4b1f-9feb-c5ba9dfdca34","f12e3f0a-43d3-4ded-b794-b7fe74929d5f","41ae5b18-dc55-46ac-a36c-e7c5327975f3","0eada61a-d059-4f89-859d-0d6e15b7b3eb","5eafe028-381e-43b8-bfd2-dc8837ecd207","2d01a72a-ba59-4fd9-82d3-f864bad31131","669ba382-8cbf-41ad-b46e-3e07fd008086","85bad58a-d116-4a9e-aea5-40c9b426844d","2ea69dd4-9f01-4d94-a454-baca15037ad7","a861f88c-3609-4c79-af1b-fa692a7a3714","d815ddb3-340d-411a-a1bd-2860381d1887","c295f1c7-454e-4d0b-b44f-62ebfb0eede6","2eb50c6d-a23a-4682-88d3-951ff0d9ce77","c377fc92-eae9-4bea-bf99-c22eb14e21aa","91542a8e-708a-44e7-a692-53de4da51e8a")
//    val eventNames = List("transaction", "level_up", "install", "start_session", "worlds", "spin", "facebook_connect")
//
//    val transnCol = List("purchase_count", "price", "session_id", "event_id")
//    val levelUpCol = List("session_id", "level", "vip_level", "event_id")
//    val installCol = List("session_id", "event_id")
//    val startSesCol = List("version", "balance", "bingo_balls_balance", "platform_id", "blackdiamond_elite_active", "session_id", "event_id", "level", "vip_level")
//    val worldsCol = List("session_id", "last_unlocked_world", "event_id")
//    val spinCol = List("bet_amount", "event_id", "session_id")
//    val facebookConnectCol = List("session_id", "fb_id", "event_id")
//
//    var mapperEventCol: Map[String, List[String]] = Map("transaction" -> transnCol, "level_up" -> levelUpCol, "install" -> installCol, "start_session" -> startSesCol, "worlds" -> worldsCol, "spin" -> spinCol, "facebook_connect" -> facebookConnectCol)
//
//    var colList: Map[String, List[String]] = Map()
//
//    for (event <- eventNames) {
//      if (event.equalsIgnoreCase("transaction")) {
//        colList = colList + (event -> transnCol)
//      }
//      if (event.equalsIgnoreCase("level_up")) {
//        colList = colList + (event -> levelUpCol)
//      }
//      if (event.equalsIgnoreCase("install")) {
//        colList = colList + (event -> installCol)
//      }
//      if (event.equalsIgnoreCase("start_session")) {
//        colList = colList + (event -> startSesCol)
//      }
//      if (event.equalsIgnoreCase("worlds")) {
//        colList = colList + (event -> worldsCol)
//      }
//      if (event.equalsIgnoreCase("spin")) {
//        colList = colList + (event -> spinCol)
//      }
//      if (event.equalsIgnoreCase("facebook_connect")) {
//        colList = colList + (event -> facebookConnectCol)
//      }
//    }
//
//
//    try {
//
//      val lastHourRead = readLastPathOnHDFS("/dgn/ovs/schema_checkpoints", spark).trim //.first().getString(0)
//
//      OPrinter.logAndPrint(s"lastHourRead is $lastHourRead")
//
//      val formatter = DateTimeFormat.forPattern("yyyy/MM/dd")
//      val currDay = DateTime.parse(lastHourRead.substring(0,lastHourRead.length-3), formatter)
//      println(currDay)
//      val nextDay= currDay.plusDays(1).toString(formatter)
//      println(nextDay)
//
//      val paths= getPathsToRead(lastHourRead, currDay, nextDay, spark)
//
//      val status = hourwiseDataReader(paths, spark, eventNames, userList, mapperEventCol, numPartitions)
//
//      OPrinter.logAndPrint(s"Data proccessed for " + status)
//    }
//    catch{
//      case e: Exception => {
//        OPrinter.logAndPrint("Spark Context Stopped")
//        spark.sparkContext.stop()
//        Notifier.sendEMail("dgn_ovs@octro.com", s"Error occurred " + e.getMessage, "simran.maurya@octrotalk.com", "Action required : Couldn't process dgn-ovs data")
//        OPrinter.logAndPrint(e.getMessage)
//      }
//    }
//
//  }
//
//  def hourwiseDataReader(hourPathToRead: List[String], spark: SparkSession, eventNames: List[String], userList: List[String], mapperEventCol: Map[String, List[String]], numPartitions: Int): String = {
//    import spark.implicits._
//
//    if (hourPathToRead.size != 0) {
//      try {
//        hourPathToRead.foreach(hour => {
//
//          //          var no_of_hits = 0
//
//          OPrinter.logAndPrint(s"Reading data for path $hour/*")
//          val jsonRDD = spark.sparkContext.textFile(s"$hour/*")
//            .flatMap(json => json.split("\\*##\\*")).map(strr => strr.slice(strr.indexOf("{"), strr.lastIndexOf("}") + 1))
//
//          val df = spark.read.json(jsonRDD).filter(col("event_type").isin(eventNames: _*) && col("user_id").isin(userList: _*))
//          //          OPrinter.logAndPrint("No. of records  " + df.count())
//
//          val schemaJson = spark.read.text("/dgn/ovs/schema_checkpoints/schema.txt").toDF("col")
//          //          schemaJson.printSchema
//
//          val splitDF = schemaJson.withColumn("column_name", functions.split($"col", ":")(0))
//            .withColumn("column_type", functions.split($"col", ":")(1))
//            .drop("col")
//          //      splitDF.show(2,false)
//
//          val colSchema = splitDF.collect().map { (row: Row) =>
//            val column = row.getString(0)
//            val datatype = row.getString(1)
//
//            // Create a map for each row
//            (column.trim(), datatype.trim())
//          }.toMap
//
//          //      colSchema.foreach(println)
//
//          val jsonData = df.toJSON.rdd
//          val repRDD= jsonData.repartition(numPartitions)
//
//          //          OPrinter.logAndPrint("RDD[String] as :")
//          //          jsonData.take(1).foreach(println)
//          var retRDD = repRDD.mapPartitions(partition => {
//            val connection = HbaseConnectionUtilProd.getConnection()
//            val useridToGocidHbaseTbl = connection.getTable(TableName.valueOf(dgn_map_userid_gocid_tbl))
//            val gocidToUseridHbaseTbl = connection.getTable(TableName.valueOf(dgn_map_gocid_userid_tbl))
//            val hashedidToGocidHbaseTbl = connection.getTable(TableName.valueOf(dgn_map_hashedid_gocid_tbl))
//
//            val newRDD = partition.map(row => {
//              if (row != null && row.contains("event_type") && row.contains("ts") && row.contains("user_id") ) {
//                //                OPrinter.logAndPrint("For row: " + row)
//                val gson = new Gson()
//                val jsonObject: JsonObject = gson.fromJson(row, classOf[JsonObject])
//
//                val event_name = jsonObject.get("event_type").getAsString
//                val epoch_time = jsonObject.get("ts").getAsLong
//                val hashed_user_id = Option(jsonObject.getAsJsonPrimitive("user_id"))
//                  .map(_.getAsString)
//                  .getOrElse("")
//                val ref_user_id = Option(jsonObject.getAsJsonPrimitive("numeric_user_id"))
//                  .map(_.getAsString)
//                  .getOrElse("")
//                val game_id = "Old Vegas Slots"
//
//                if(hashed_user_id == "") {
//                  Notifier.sendEMail("dgn_ovs@octro.com", s"Data packet doesn't contain user_id for \n : $row", "simran.maurya@octrotalk.com", "Action required : Couldn't process dgn-ovs data")
//                }
//
//                val rowkey = hashed_user_id.trim()
//                var getUserData = (rowkey, hashedidToGocidHbaseTbl.get(new Get(Bytes.toBytes(rowkey))))
//
//                //                OPrinter.logAndPrint(s"Get data from hbase table $ref_user_id")
//                var gocid_from_hbase = ""
//                try {
//                  gocid_from_hbase = Bytes.toString(getUserData._2.getValue(Bytes.toBytes("data"), Bytes.toBytes("goc_id"))).toString()
//                } catch {
//                  case e: Exception => {
//                    //                    OPrinter.logAndPrint("Inside catch")
//                  }
//                }
//                //                OPrinter.logAndPrint(s"Get data from hbase table $gocid_from_hbase")
//
//                var hbasePut = new Put(Bytes.toBytes(hashed_user_id))
//                val event_time = Format.epochToDateTime(epoch_time)
//
//                val colList = mapperEventCol(event_name)
//
//                //            if(event_name.equalsIgnoreCase("level_up")){
//                var eventValueJson = new JsonObject
//                for (col <- colList) {
//                  //                  OPrinter.logAndPrint(s"$col: $colSchema.get(col)")
//                  val dataType = colSchema(col)
//                  val extractedValue: Any = dataType match {
//                    case "string" => {
//                      Option(jsonObject.get(col)) match {
//                        case Some(jsonElement) =>
//                          val value = jsonElement.getAsString
//                          eventValueJson.add(col, new JsonPrimitive(value))
//                        case None =>
//                          eventValueJson.add(col, JsonNull.INSTANCE)
//                      }
//                    }
//                    case "long" => {
//                      Option(jsonObject.get(col)) match {
//                        case Some(jsonElement) =>
//                          val value = jsonElement.getAsLong
//                          eventValueJson.add(col, new JsonPrimitive(value))
//                        case None =>
//                          eventValueJson.add(col, JsonNull.INSTANCE)
//                      }
//                    }
//                    case "boolean" => {
//                      Option(jsonObject.get(col)) match {
//                        case Some(jsonElement) =>
//                          val value = jsonElement.getAsBoolean
//                          eventValueJson.add(col, new JsonPrimitive(value))
//                        case None =>
//                          eventValueJson.add(col, JsonNull.INSTANCE)
//                      }
//                    }
//                    case "double" => {
//                      Option(jsonObject.get(col)) match {
//                        case Some(jsonElement) =>
//                          val value = jsonElement.getAsDouble
//                          eventValueJson.add(col, new JsonPrimitive(value))
//                        case None =>
//                          eventValueJson.add(col, JsonNull.INSTANCE)
//                      }
//                    }
//                    case "array" => {
//                      Option(jsonObject.get(col)) match {
//                        case Some(jsonElement) =>
//                          val value = jsonElement.getAsJsonObject
//                          eventValueJson.add(col, value)
//                        case None =>
//                          eventValueJson.add(col, JsonNull.INSTANCE)
//                      }
//                    }
//                    case _ => {
//                      //                      OPrinter.logAndPrint(s"for column $col else datatype: $dataType \n row $row")
//                      throw new IllegalArgumentException(s"Unsupported data type: $dataType")
//                    }
//                  }
//
//                }
//                eventValueJson.add("epoch_time", new JsonPrimitive(epoch_time))
//                eventValueJson.add("event_time", new JsonPrimitive(event_time))
//                eventValueJson.add("user_id_hashed", new JsonPrimitive(hashed_user_id))
//                eventValueJson.add("user_id_numeric", new JsonPrimitive(ref_user_id))
//
//
//                var goc_id = ""
//                if (gocid_from_hbase != "") {
//                  //                  OPrinter.logAndPrint("Inside if line 251: " + gocid_from_hbase)
//                  goc_id = gocid_from_hbase
//                }
//                else {
//                  //                  OPrinter.logAndPrint("inside else")
//                  //                  no_of_hits = no_of_hits + 1
//                  val response = getGOCIDfromUSERID(hashed_user_id)
//                  val outerJson = stringToJsonConverter(response)
//
//                  val innerJson = stringToJsonConverter(outerJson.get("body").getAsString)
//                  //                  println("innerJson "+innerJson)
//                  val goc_id = innerJson.get("body").getAsJsonObject.get("GOCID").getAsString
//                  //                  val goc_id= getGOCIDfromUSERID(ref_user_id)
//
//                  if (goc_id != "") {
//                    //                OPrinter.logAndPrint("Inside if line 262: " + goc_id)
//                    hbasePut.addColumn("data".getBytes, "goc_id".getBytes, goc_id.toString().getBytes)
//                    hbasePut.addColumn("data".getBytes, "numeric_id".getBytes, ref_user_id.toString().getBytes)
//                    //              useridToGocidHbaseTbl.put(hbasePut)
//                    hashedidToGocidHbaseTbl.put(hbasePut)
//                  }
//
//                  // Now putting user_id corresponding to this goc_id in dgn_map_gocid_userid_tbl
//                  var hbasePut2 = new Put(Bytes.toBytes(goc_id))
//                  hbasePut2.addColumn("data".getBytes, "numeric_id".getBytes, ref_user_id.toString().getBytes)
//                  hbasePut2.addColumn("data".getBytes, "hashed_id".getBytes, hashed_user_id.toString().getBytes)
//                  gocidToUseridHbaseTbl.put(hbasePut2)
//
//                  //            var hbasePut3 = new Put(Bytes.toBytes(hashed_user_id))
//                  if(ref_user_id !=""){
//                    var hbasePut3 = new Put(Bytes.toBytes(ref_user_id))
//                    hbasePut3.addColumn("data".getBytes, "hashed_id".getBytes, hashed_user_id.toString().getBytes)
//                    hbasePut3.addColumn("data".getBytes, "goc_id".getBytes, goc_id.toString().getBytes)
//                    useridToGocidHbaseTbl.put(hbasePut3)
//                  }
//                }
//
//                val mainJson = new JsonObject
//                mainJson.addProperty("game_id", game_id)
//                mainJson.addProperty("event_time", event_time)
//                mainJson.addProperty("event_name", event_name)
//                mainJson.addProperty("user_id", goc_id)
//                mainJson.add("event_value", eventValueJson)
//
//                //                OPrinter.logAndPrint("Value of no. of API hits for this hour: " + no_of_hits)
//                (mainJson.toString, game_id, goc_id, event_time, event_name, eventValueJson.toString)
//                //            (event_name, event_time, game_id, hashed_user_id, eventValueJson.toString)
//              }
//              else {
//                Notifier.sendEMail("dgn_ovs@octro.com", s"Data packet is missing some key for \n : $row", "simran.maurya@octrotalk.com", "Action required : Couldn't process dgn-ovs data")
//                //            OPrinter.logAndPrint("Else row" + row)
//                ("", "", "", "", "", "")
//              }
//            }).filter(x => x != null && x._1 != null && x._1 != "" && x._2 != null && x._2 != "")
//            newRDD
//          }).filter(row => {
//            val jsoStr = stringToJsonConverter(row._1)
//            jsoStr.has("user_id") && jsoStr.get("user_id").getAsString.nonEmpty
//          }).persist()
//
//          OPrinter.logAndPrint(s"Pushing $hour hour data to kafka topic $dgnOvsTopic")
//          OKafkaProducer.pushToTopic(dgnOvsTopic, retRDD.map(x => x._1))
//          OPrinter.logAndPrint(s"Data saved to kafka topic $dgnOvsTopic")
//
//          val lastReadDateHour= hour.substring(hour.size-13)
//
//          OPrinter.logAndPrint(s"Writing data on hdfs for $lastReadDateHour")
//          retRDD.map(x => x._1).saveAsTextFile(s"/dgn/ovs/kafka_events_correction/$lastReadDateHour")
//          OPrinter.logAndPrint(s"Data saved on hdfs")
//
//          OPrinter.logAndPrint("Writing last read path on HDFS" + "!!")
//          writeLastReadPathOnHDFS(s"$lastReadDateHour", "/dgn/ovs/schema_checkpoints", spark)
//          //          retRDD.unpersist()
//          retRDD.unpersist()
//        })
//      }
//      catch {
//        case e: Exception => {
//          OPrinter.logAndPrint("Spark Context Stopped")
//          spark.sparkContext.stop()
//          //          val stackTrace = "com.octro.start.main.StartMain :: " + ExceptionUtils.getStackTrace(e) + "<br/><br/>"
//          //          Notifier.sendEMail("dgn_ovs@octro.com", s"Error occurred in dgn_ovs connector for date $start_date hour $hour " + "\n<br/>" + e.getMessage + "\n<br/>stackTrace = " + stackTrace, "simran.maurya@octrotalk.com", "Action required : Couldn't process dgn-ovs data")
//          Notifier.sendEMail("dgn_ovs@octro.com", s"Error occurred in dgn_ovs connector" + e.getMessage, "simran.maurya@octrotalk.com", "Action required : Couldn't process dgn-ovs data")
//          OPrinter.logAndPrint(e.getMessage)
//        }
//      }
//    }
//    "\n done"
//  }
//
//  def getPathsToRead(lastHourRead:String, currDay: DateTime, nextDay: String, spark: SparkSession): List[String] = {
//
//    val hadoopConf = new org.apache.hadoop.conf.Configuration()
//    hadoopConf.set("fs.defaultFS", "hdfs://octropc")
//    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
//    val formatter = DateTimeFormat.forPattern("yyyy/MM/dd")
//    val toRead= currDay.toString(formatter)
//    println("toRead", toRead)
//    // val fs = FileSystem.get(new Configuration())
//    val currentList = hdfs.listStatus(new Path(s"/dgn/ovs/raw_events/ovs/events/prod/$toRead"))
//
//    // Fetch all paths from todayList and add it into a list
//    val filePaths =ListBuffer[String]()
//    currentList.foreach(x=>{
//      filePaths += x.getPath.toString
//    })
//
//    // If lastHourRead is 22 or 23, then we will fetch paths from next day  also along with the
//    if(lastHourRead.substring(lastHourRead.length-2) == "22" || lastHourRead.substring(lastHourRead.length-2) =="23"){
//      val currentPlusOneList = hdfs.listStatus(new Path(s"/dgn/ovs/raw_events/ovs/events/prod/$nextDay"))
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
//      Notifier.sendEMail("dgn_ovs@octro.com", s"There must be some lag on DGN-OVS data copying on HDFS " , "simran.maurya@octrotalk.com,amar.singh@octro.com", "Action required : Check for lag in DGN_OVS paths on hdfs")
//      OPrinter.logAndPrint("Spark Context Stopped")
//      spark.sparkContext.stop()
//    }
//    //    println(fileListAfterSubstring(0))
//
//    val todayDay = new DateTime()
//    val currentHour = todayDay.toString(Format.hourFmt).replace("-","/")
//
//    // Filter out the currentHourPath from list of all paths to be read (as data must not be prepared completely for currentHou)
//    val filterList= fileListAfterSubstring.filter(!_.contains(currentHour))     //fileList.dropWhile(!_.contains(str)).drop(1).filter(!_.contains(currentHourPath))
//    println("FilterList:")
//    filterList.foreach(println)
//    if(filterList.size < 1){
//      Notifier.sendEMail("dgn_ovs@octro.com", s"There must be some lag on DGN-OVS data copying on HDFS " , "simran.maurya@octrotalk.com", "Action required : Check for lag in DGN_OVS paths on hdfs")
//      OPrinter.logAndPrint("Spark Context Stopped")
//      spark.sparkContext.stop()
//    }
//    //    val lastPathToRead= filterList(filterList.length-1)
//    //    println("\n",lastPathToRead)
//    //    println(lastPathToRead.substring(lastPathToRead.size-13))
//
//    filterList
//  }
//
//
//  def writeLastReadPathOnHDFS(hour: String, path: String, sparkSession: SparkSession): Unit = {
//    import sparkSession.implicits._
//    val hourDf = List(hour).toDF("value").coalesce(1)
//    //    hourDf.write.mode(SaveMode.Overwrite).text(s"${path}/dgn_ovs_lastReadHour.txt")
//    hourDf.write.mode(SaveMode.Overwrite).text(s"${path}/dgn_ovs_lastReadHour.txt")
//  }
//
//  def readLastPathOnHDFS(path: String, sparkSession: SparkSession) = {
//    import sparkSession.implicits._
//    val hourDf = sparkSession.read.text(s"${path}/dgn_ovs_lastReadHour.txt").collect().head.getString(0)
//    hourDf
//  }
//
//
//
//  def getGOCIDfromUSERID(userId: String): String = {
//    val gocidEndPoint = "https://nczs65s12a.execute-api.us-east-1.amazonaws.com/v1/userprovision/guestReg"
//    //"http://serv117.octro.net:4004/userprovision/guestReg" //"http://serv117.octro.com:4004/userprovision/guestReg"
//    val action = "guestRegister"
//    val data_from = "dwbsid"
//    val guestId = userId
//    val appid = "20240221VXIz" // OVS app_id
//
//    try {
//      val response = Http(gocidEndPoint)
//        .header("content-type", "application/json")
//        .postData(s"""{"action":"guestRegister","appid":"20240221VXIz","guestId":"$guestId","data_from":"dwbsid"}""")
//        .asString
//
//      //    println("API response body = " + response.body)
//      //    println("API response code = " + response.code)
//      val res_body = response.body
//      res_body
//    }
//    catch {
//      case e: Exception => {
//        val stackTrace = "com.octro.start.main.StartMain :: " + ExceptionUtils.getStackTrace(e) + "<br/><br/>"
//        Notifier.sendEMail("dgn_ovs@octro.com", s"Error occurred while fetching gocid for userid $guestId  " + "\n<br/>" + e.getMessage + "\n<br/>stackTrace = " + stackTrace, "simran.maurya@octrotalk.com", "Action required : Couldn't fetch gocid from api")
//        ""
//      }
//    }
//    //    println("res_body type : "+res_body.getClass().getSimpleName())
//  }
//
//  def epochToPacificTime(epoch: Long): String = {
//    // Convert epoch time to DateTime
//    val dateTime = new DateTime(epoch, DateTimeZone.UTC)
//
//    // Set the time zone to Chicago
//    val pacificTimeZone = DateTimeZone.forID("America/Los_Angeles")
//    val pacificDateTime = dateTime.withZone(pacificTimeZone)
//
//    // Format DateTime as a string
//    val formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
//    val pacificTimeString = formatter.print(pacificDateTime)
//    val dateTimeString = formatter.print(dateTime) // UTC String
//
//    pacificTimeString
//  }
//
//  private def stringToJsonConverter(json: String): JsonObject = {
//    val jsonParser = new JsonParser()
//    val json_element = jsonParser.parse(json)
//    val json_data = json_element.getAsJsonObject
//    json_data
//  }
//
//}
//
//
//
