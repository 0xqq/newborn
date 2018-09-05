package com.jh.learn.flink

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._

/**
  * Created by chenjiehao on 2018/9/5
  */
object FlinkTest {
  case class Person(id:Int,name:String,age:Int)
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

  }
}
