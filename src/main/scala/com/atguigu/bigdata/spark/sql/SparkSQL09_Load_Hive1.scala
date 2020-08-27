package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._

object SparkSQL09_Load_Hive1 {

  def main(args: Array[String]): Unit = {

    // TODO 创建环境对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    // builder 构建，创建
    // TODO 默认情况下SparkSQL支持本地(内置)Hive操作，执行前需要启用hive支持 .enableHiveSupport()
    val spark = SparkSession.builder()
      .enableHiveSupport()  // 启用hive支持  会在根目录创建 metastore_db和spark-warehouse文件夹
      .config(sparkConf).getOrCreate()
    // 导入隐式转换，这里的spark其实是环境对象的名称
    // 要求这个对象使用val声明

    // TODO SparkSQL
//    spark.sql("create table aa(id int)")
//    spark.sql("show tables").show()
    spark.sql("load data local inpath 'input/loadHive_id.txt' into table aa")
    spark.sql("select * from aa").show()

    // TODO 释放对象
    spark.stop()
  }

}
