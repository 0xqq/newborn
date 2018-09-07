package com.jh.learn.spark.streaming

import java.util

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by chenjiehao on 2018/9/7
  */
object SparkStreamingDemo {
  val MYSQL_CALC_REGTIME = "calla.reg_time"
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().master("yarn")
      .appName("XXXXX")
      .config("spark.dynamicAllocation.enabled", "false") // 控制自动分配资源
      .config("spark.default.parallelism", "30")
      .config("spark.shuffle.file.buffer", "64k")
      .config("spark.reducer.maxSizeInFlight", "96m")
      .config("spark.streaming.backpressure.enabled", true)  //反压机制的开启
      .config("spark.seriailzer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    val ssc = new StreamingContext(session.sparkContext, Seconds(2))

    val aryTops = new util.ArrayList[String]()
    aryTops.add("jm-jxl")
    val kafkaParams = getkafkaParms()
    val messages: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe(aryTops, kafkaParams))

    messages.foreachRDD { record => {
      try {
        val rdd: RDD[String] = record.map(_.value())
        //解析basic下reg_time字段
        parseRegTime(session, rdd)
      } catch {
        case e: Exception =>e.printStackTrace()

      }
    }
    }
    ssc.start()
    ssc.awaitTermination()
  }

  def getkafkaParms(): util.HashMap[String, Object] = {
    val kafkaParam = new util.HashMap[String, Object]()
//    val brokers = LoadConfig.getProperties("brokers").toString
    kafkaParam.put("metadata.broker.list", "XXXXX")
    kafkaParam.put("bootstrap.servers", "XXXXXXXX")
    kafkaParam.put("group.id", "callRestDistStreaming")
    kafkaParam.put("max.partition.fetch.bytes", "50000000")
    kafkaParam.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaParam.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaParam
  }

  def parseRegTime(session: SparkSession, rdd: RDD[String]): Unit = {
    val result = session.read.json(rdd).na.drop(Array("identity"))
      .selectExpr("identity", "cast(round(datediff(from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss'),data.basic.reg_time)/30) as int) as inNetTime")

    result.collect().foreach(row => {
      val identity = row.getAs[String]("identity")
      val reg_time = row.getAs[Int]("inNetTime")

      //插入mysql
      val sql = new StringBuffer(s"INSERT INTO $MYSQL_CALC_REGTIME ( identity, in_net_time, input_date_time, last_update_time ) \n")
      sql.append(" VALUES (?, ?, NOW(), NOW() ) \n")
        .append(" ON DUPLICATE KEY UPDATE in_net_time = ?, last_update_time = NOW()")
//      C3p0Pools.execute(sql.toString, Array(identity, reg_time, reg_time))
    }
    )
    //下列这种方式，测试的时候没有通过。有时候有数据写入，但是好像数据也丢失的现象
    //    result.foreachPartition(row => {
    //      row.foreach(row => {
    //        val identity = row.getAs[String]("identity")
    //        val reg_time = row.getAs[Int]("inNetTime")
    //        println(identity + "=========")
    //        println(reg_time + "")
    //        //插入mysql
    //        val sql = new StringBuffer(s"INSERT INTO $MYSQL_CALC_REGTIME ( identity, in_net_time, input_date_time, last_update_time ) \n")
    //        sql.append(" VALUES (?, ?, NOW(), NOW() ) \n")
    //          .append(" ON DUPLICATE KEY UPDATE in_net_time = ?, last_update_time = NOW()")
    ////        writeLogs("apc","" + identity + "..." + reg_time, sql.toString)
    //        C3p0Pools.execute(sql.toString, Array(identity, reg_time,reg_time))
    //      })
    //
    //    })
  }

}
