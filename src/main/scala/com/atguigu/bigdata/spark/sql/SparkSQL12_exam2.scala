package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._

object SparkSQL12_exam2 {

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

    // TODO SparkSQL
//    spark.sql("use atguigu200720")  // 外置hive的database，load数据时，table需要 eg：atguigu200720.user_visit_action
    spark.sql(
      """
        |select
        |    *
        |from (
        |    select
        |        *,
        |        rank() over(partition by area order by clickCount desc) as rank
        |    from (
        |        select
        |            area,
        |            product_name,
        |            count(*) as clickCount
        |        from (
        |            select
        |                a.*,
        |                c.area,
        |                p.product_name
        |            from user_visit_action a
        |            join city_info c on c.city_id = a.city_id
        |            join product_info p on p.product_id = a.click_product_id
        |            where a.click_product_id > -1
        |        ) t1 group by area, product_name
        |    ) t2
        |) t3
        |where rank <= 4
        |""".stripMargin).show()

    // TODO 释放对象
    spark.stop()
  }

}
