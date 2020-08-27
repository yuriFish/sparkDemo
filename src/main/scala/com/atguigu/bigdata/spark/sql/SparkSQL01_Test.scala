package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkSQL01_Test {

  def main(args: Array[String]): Unit = {

    // TODO 创建环境对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    // builder 构建，创建
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    // 导入隐式转换，这里的spark其实是环境对象的名称
    // 要求这个对象使用val声明
    import spark.implicits._

    // TODO 逻辑操作

    // TODO SQL
    val jsonDF: DataFrame = spark.read.json("input/user.json")
    // TODO DSL
    // 将df转换为临时视图
    jsonDF.createOrReplaceTempView("user")
//    spark.sql("select * from user").show
    // 如果查询列名采用单引号，那么需要隐式转换   import spark.implicits._
//    jsonDF.select("name", "age").show
//    jsonDF.select('name, 'age).show

    // TODO RDD <==> DataFrame
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List(
      (1, "zhangsan", 30),
      (2, "lisi", 28),
      (3, "wangwu", 20)
    ))

    val df: DataFrame = rdd.toDF("id", "name", "age")
    val dfToRDD: RDD[Row] = df.rdd

    // TODO RDD <==> DataSet
    val userRDD: RDD[User] = rdd.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }

    val userDS: Dataset[User] = userRDD.toDS()
    val dsToRDD: RDD[User] = userDS.rdd

    // TODO DataFrame <==> DataSet
    val dfToDS: Dataset[User] = df.as[User]
    val dsToDF: DataFrame = dfToDS.toDF()

    println("rdd---------")
    rdd.foreach(println)
    println("df---------")
    df.show()
    println("userDS---------")
    userDS.show()

    // TODO 释放对象
    spark.stop()
  }

  case class User(id:Int, name:String, age:Int)
}
