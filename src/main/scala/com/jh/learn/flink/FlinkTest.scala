//package com.jh.learn.flink
//
//import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
//import org.apache.flink.table.api.TableEnvironment
//import org.apache.flink.api.scala._
//import org.apache.flink.table.api.scala._
//
///**
//  * Created by chenjiehao on 2018/9/5
//  */
//object FlinkTest {
//  case class Person(id:Int,name:String,age:Int)
//  def main(args: Array[String]): Unit = {
//
//    // set up execution environment
////    val env = ExecutionEnvironment.getExecutionEnvironment
//    val env = ExecutionEnvironment.getExecutionEnvironment
//    val tEnv = TableEnvironment.getTableEnvironment(env)
//
////    val tableEnvironment = TableEnvironment.getTableEnvironment(env)
//    val result: DataSet[Person] = env.readTextFile("/Users/chenjiehao/Desktop/flinkTabel.txt").map(v => {
//      val ops = v.split("")
//      Person(Integer.parseInt(ops(0)), ops(1), Integer.parseInt(ops(2)))
//    })
//
////    val table = tEnv.fromDataSet(result)
//
//
////    tEnv.registerTable("test",table)
//
////    tEnv.sqlQuery("select * from test")
////    tEnv.sqlQuery("select * from test").
////    result.print()
////    tEnv.registerDataSet("testTable",result)
//
////    tEnv.sqlQuery("select * from testTable").printSchema()
//
//  }
//}
