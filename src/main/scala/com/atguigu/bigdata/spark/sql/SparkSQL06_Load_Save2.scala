package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._

object SparkSQL06_Load_Save2 {

  def main(args: Array[String]): Unit = {

    // TODO 创建环境对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    // builder 构建，创建
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    // 导入隐式转换，这里的spark其实是环境对象的名称
    // 要求这个对象使用val声明

    // TODO SparkSQL通用的读取和保存
    val df: DataFrame = spark.sql("select * from json.`input/user.json`")
    df.show()

    df.write.mode("append").json("output/sparkSQL_Load_Save2")

    // TODO 释放对象
    spark.stop()
  }

}
