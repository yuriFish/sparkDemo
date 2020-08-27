package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._

object SparkSQL08_LoadSave_MySQL {

  def main(args: Array[String]): Unit = {

    // TODO 创建环境对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    // builder 构建，创建
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    // 导入隐式转换，这里的spark其实是环境对象的名称
    // 要求这个对象使用val声明
    import spark.implicits._

    // TODO SparkSQL jdbc的读取和保存
    val df: DataFrame = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/spark-sql")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "1234")
      .option("dbtable", "user")
      .load()

    df.show()

    df.write.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/spark-sql")
      .option("user", "root")
      .option("password", "1234")
      .option("dbtable", "user1") // 保存新的表user1
      .mode(SaveMode.Append)
      .save()


    // TODO 释放对象
    spark.stop()
  }

}
