package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._

object SparkSQL11_exam1 {

  def main(args: Array[String]): Unit = {

    // TODO 创建环境对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    // builder 构建，创建
    // TODO 默认情况下SparkSQL支持本地(内置)Hive操作，执行前需要启用hive支持 .enableHiveSupport()
    // TODO 访问外置的Hive时，需要将(虚拟机上)配置好的hive-site.xml文件放在 src/main/resources 和 target/classes文件夹下 并且hive要启动
    val spark = SparkSession.builder()
      .enableHiveSupport()  // 启用hive支持  会在根目录创建 metastore_db和spark-warehouse文件夹
      .config(sparkConf).getOrCreate()
    // 导入隐式转换，这里的spark其实是环境对象的名称
    // 要求这个对象使用val声明
    import spark.implicits._

    // TODO SparkSQL
    // TODO 创建表
//    spark.sql("use atguigu200720")  // 外置hive的database，load数据时，table需要 eg：atguigu200720.user_visit_action
    spark.sql(
      """
        |CREATE TABLE `user_visit_action`(
        |  `date` string,
        |  `user_id` bigint,
        |  `session_id` string,
        |  `page_id` bigint,
        |  `action_time` string,
        |  `search_keyword` string,
        |  `click_category_id` bigint,
        |  `click_product_id` bigint,
        |  `order_category_ids` string,
        |  `order_product_ids` string,
        |  `pay_category_ids` string,
        |  `pay_product_ids` string,
        |  `city_id` bigint)
        |row format delimited fields terminated by '\t'
        |""".stripMargin
    )
    spark.sql(
      """
        |load data local inpath 'input_sparkSQL_exam/user_visit_action.txt' into table user_visit_action
        |""".stripMargin
    )

    spark.sql(
      """
        |CREATE TABLE `product_info`(
        |  `product_id` bigint,
        |  `product_name` string,
        |  `extend_info` string)
        |row format delimited fields terminated by '\t'
        |""".stripMargin
    )
    spark.sql(
      """
        |load data local inpath 'input_sparkSQL_exam/product_info.txt' into table product_info
        |""".stripMargin
    )

    spark.sql(
      """
        |CREATE TABLE `city_info`(
        |  `city_id` bigint,
        |  `city_name` string,
        |  `area` string)
        |row format delimited fields terminated by '\t'
        |""".stripMargin
    )
    spark.sql(
      """
        |load data local inpath 'input_sparkSQL_exam/city_info.txt' into table city_info
        |""".stripMargin
    )

    spark.sql(
      """
        |select * from city_info
        |""".stripMargin).show(10)

    // TODO 释放对象
    spark.stop()
  }

}
