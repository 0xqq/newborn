package com.jh.learn.util

import scala.collection.mutable.ArrayBuffer

/**
  * Created by chenjiehao on 2018/9/5
  */
object Substring_index {
  /**
    *
    * @param str   传递的字符串
    * @param key   关键字符 主要要是使用转义如"\\."  "\\?"
    * @param number 关键字出现的次数
    * @return
    */
  def test(str:String,key:String,number:Int): String ={
    var result=""
    val r=str.split(key) //以关键字作为切割，生成数组

    //验证关键字出现的次数
    if(number>=r.size||Math.abs(number)>=r.size){
      return "error"
    }

    //次数为正负的考虑
    if(number>0){
      for(i <- 0 until  number){
        if(i==number-1){
          result+=r(i)
        }else{
          result+=r(i)+key.replace("\\","")
        }
      }
      return result
    }else{
      val array=ArrayBuffer[String]()
      for(i <- ( r.size-Math.abs(number) to Math.abs(number)).reverse){
        if(i==r.size-Math.abs(number)){
          array+=r(i)
        }else{
          array+=key.replace("\\","")+r(i)
        }
      }
      return array.reverse.mkString
    }
  }
}
