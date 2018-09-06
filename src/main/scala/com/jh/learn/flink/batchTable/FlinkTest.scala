package com.jh.learn.flink.batchTable

import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

/**
  * Created by chenjiehao on 2018/9/5
  */
object FlinkTest {

  case class Person(id: Int, name: String, age: Int)

  def main(args: Array[String]): Unit = {
    /**
      * 主要是对比flink读取文件映射成表与spark读取文件映射表的区别！
      *
      * 期间问题：
      * 1：使用scala写flink的时候，需要主要在pom文件添加的flink的依赖应该是和scala相关的。
      * 2：使用scala操作flink table的时候应该import org.apache.flink.api.scala._ & org.apache.flink.table.api.scala._
      */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    /**
      * source的方式
      * 1：readTextFile
      * 2：readFileOfPrimitives
      */
    println(env.getParallelism+"#########") //查看默认情况的并行度
    //当设置并行度为1的时候，就可以保证读取文件的有序。
    val unit = env.readTextFile("/Users/chenjiehao/Desktop/flinkTabel.txt").setParallelism(1)
    val unit2 = env.readFileOfPrimitives[Person]("/Users/chenjiehao/Desktop/flinkTabel.txt")

    var t = unit.map(r => {
      val strings = r.split(",")
      Person(Integer.parseInt(strings(0)), strings(1), Integer.parseInt(strings(2)))
    })

    val table = tEnv.fromDataSet(t).select("*")
    tEnv.registerTable("test", table)

    val table1 = tEnv.fromDataSet(t).select("*")
    tEnv.registerTable("test2", table1)

    //将table转换成DataSet进行输出查看。这里就需要import上述的依赖包
    tEnv.sqlQuery("select id,name from test").toDataSet[Row].print()
    //结果如下
/*
    8,"zhangshan"
    1,"zhangshan"
    2,"zhangshan"
    3,"zhangshan"
    6,"zhangshan"
    7,"zhangshan"
    4,"zhangshan"
    5,"zhangshan"
    */
    //问题，为什么查询的结果不是有序的？ 因为默认flink的并行度的是总cpu的核数。
    tEnv.sqlQuery("select id,name from test2").toDataSet[Row].print()
  }
}
