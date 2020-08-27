package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row, SparkSession}

object SparkSQL04_UDAF_Class {

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
//    df.createOrReplaceTempView("user")
    val ds: Dataset[User] = df.as[User]

    // TODO 创建UDAF函数
    val udaf = new MyAvgAgeUDAFClass

    // TODO 注册到SparkSQL中
//    spark.udf.register("avgAge", udaf)

    // TODO 在SQL中使用聚合函数
    // 因为聚合函数是强类型，那么sql中没有类型的概念，所以无法使用 spark.udf.register("avgAge", udaf)
    // 可以采用DSL语法进行访问
     //将聚合函数转换为查询的列让DataSet访问
    ds.select(udaf.toColumn).show()

    // TODO 释放对象
    spark.stop()
  }

  case class User(id:Int, name:String, age:Long)
  case class AvgBuffer(var totalAge:Long, var count:Long)

  // 自定义聚合函数 -- 强类型
  // 1. 继承Aggregator[-IN, BUF, OUT]
  //    -IN, 输入数据的类型 User
  //    BUF, 缓冲区的数据类型 AvgBuffer
  //    OUT, 输出数据的类型 Long
  // 2. 重写方法
  class MyAvgAgeUDAFClass extends Aggregator[User, AvgBuffer, Long]{
    // TODO 缓冲区的初始值
    override def zero: AvgBuffer = AvgBuffer(0L, 0L)

    // TODO 聚合数据
    override def reduce(buffer: AvgBuffer, user: User): AvgBuffer = {
      buffer.totalAge = buffer.totalAge + user.age
      buffer.count = buffer.count + 1
      buffer
    }

    // TODO 合并缓冲区
    override def merge(buffer1: AvgBuffer, buffer2: AvgBuffer): AvgBuffer = {
      buffer1.totalAge = buffer1.totalAge + buffer2.totalAge
      buffer1.count = buffer1.count + buffer2.count
      buffer1
    }

    // TODO 计算函数结果
    override def finish(reduction: AvgBuffer): Long = {
      reduction.totalAge / reduction.count
    }

    override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product

    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }

}
