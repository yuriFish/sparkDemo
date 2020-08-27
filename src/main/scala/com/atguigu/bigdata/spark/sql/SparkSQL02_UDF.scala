package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkSQL02_UDF {

  def main(args: Array[String]): Unit = {

    // TODO 创建环境对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    // builder 构建，创建
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    // 导入隐式转换，这里的spark其实是环境对象的名称
    // 要求这个对象使用val声明
    import spark.implicits._

    // TODO 逻辑操作
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List(
      (1, "zhangsan", 30),
      (2, "lisi", 28),
      (3, "wangwu", 20)
    ))
    val df: DataFrame = rdd.toDF("id", "name", "age")
    df.createOrReplaceTempView("user")

    // 报错
//    val df: DataFrame = rdd.toDF("id", "name", "age")
//    val ds: Dataset[Row] = df.map(row => {
//      val id = row(0)
//      val name = row(1)
//      val age = row(2)
//      Row(id, "name : " + name, age)
//    })
//    ds.show()

    // 没报错
//    val userRDD: RDD[User] = rdd.map {
//      case (id, name, age) => {
//        User(id, name, age)
//      }
//    }
//    val userDS: Dataset[User] = userRDD.toDS()
//    val newUserDS: Dataset[User] = userDS.map(
//      user => {
//        User(user.id, "name : " + user.name, user.age)
//      }
//    )
//    newUserDS.show()


    // TODO SparkSQL封装的对象封装了大量的方法进行数据的处理，类似于RDD的算子操作
//    df.join()
//    df.reduce()
//    df.coalesce()

    // TODO 使用自定义函数在SQL中完成数据的转换操作
    spark.udf.register("addName", (x:String) => "Name: " + x)
    spark.udf.register("changeAge", (x:Int) => 18)
    spark.sql("select addName(name), changeAge(age) from user").show()


//    spark.sql("select addName(name), avg(age) from user").show()    // 报错，需要自定义聚合函数 在 SparkSQL03_UDAF

    // TODO 释放对象
    spark.stop()
  }

  case class User(id:Int, name:String, age:Int)
}
