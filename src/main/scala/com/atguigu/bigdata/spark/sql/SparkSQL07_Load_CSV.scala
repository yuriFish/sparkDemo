package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._

object SparkSQL07_Load_CSV {

  def main(args: Array[String]): Unit = {

    // TODO 创建环境对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    // builder 构建，创建
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    // 导入隐式转换，这里的spark其实是环境对象的名称
    // 要求这个对象使用val声明

    // TODO SparkSQL csv的读取和保存
    val df: DataFrame = spark.read.format("csv")
      .option("sep", ";")   // 分隔符
      .option("inferSchema", "true")
      .option("header", "true")
      .load("input/user.csv")
    df.show()

    // TODO 释放对象
    spark.stop()
  }

}
