package com.octro.utilities

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}

object HbaseConnectionUtilProd {

  /* NOTE :
   * Make sure that hbase-site.xml is sourced instead of manually calling conf.set() for hbase.zookeeper.quorum, etc.
   * */
  val conf: Configuration = HBaseConfiguration.create()

//  conf.set("fs.defaultFS", "hdfs://octropc"); // nameservices
//  conf.set("dfs.nameservices", "octropc");
//  conf.set("dfs.ha.namenodes.octropc", "hmaster,hmaster22,hmaster24"); //namenodes
//  conf.set("dfs.namenode.rpc-address.octropc.hmaster", "hmaster:9000")
//  conf.set("dfs.namenode.rpc-address.octropc.hmaster22", "hmaster22:9000")
//  conf.set("dfs.namenode.rpc-address.octropc.hmaster24", "hmaster24:9000")
//  conf.set("dfs.client.failover.proxy.provider.octropc", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")
//  conf.set("hbase.rootdir", "hdfs://octropc/hbase")
//  conf.set("hbase.zookeeper.quorum", "hmaster22,hmaster24,hmaster")
//  conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.addResource(new Path("$HBASE_HOME/conf/hbase-site.xml"))


  val connection = ConnectionFactory.createConnection(this.conf)

  def getConnection(): Connection = {
    this.connection
  }

  def getConf(): Configuration = {
    this.conf
  }
}
