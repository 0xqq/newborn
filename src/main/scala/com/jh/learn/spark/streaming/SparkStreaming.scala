package com.jh.learn.spark.streaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Second
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

/**
  * Created by chenjiehao on 2018/9/6
  */
object SparkStreaming {
  /**
    * sparkStreaming集成kafka,完善数据消费的exactly-once
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean) //确保kafka禁止自动提交
    )


    val session = SparkSession.builder().appName("SparkStreaming").master("local[2]").getOrCreate()
    val scc = new StreamingContext(session.sparkContext, Seconds(2))
    val topics = Array("topicA", "topicB")

    val stream=KafkaUtils.createDirectStream(scc, PreferConsistent, Subscribe(topics, kafkaParams))

    /**
      * 多种方式保存offset位置，官网提供
      * 1：checkpiont
      * 2：kafka itself
      * 3：your own data store
      *
      */
    //使用kafka itself自己保存相关offset信息
    stream.foreachRDD(rdd=>{
      //获取offsets
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition { iter =>
        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      }
      //逻辑处理完成后，手动触发offset的commit。
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    })

  }
}
