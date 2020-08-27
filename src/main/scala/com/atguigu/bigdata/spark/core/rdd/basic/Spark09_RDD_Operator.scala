package com.atguigu.bigdata.spark.core.rdd.basic

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark09_RDD_Operator {

  def main(args: Array[String]): Unit = {

    // TODO Spark - RDD - 算子(方法)

    val sparkConf = new SparkConf().setMaster("local").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    // 旧RDD => 算子 => 新RDD
//    val rdd1: RDD[Int] = rdd.map(i => { i*2 })
//    val rdd1: RDD[Int] = rdd.map(i => i*2)
    val rdd1: RDD[Int] = rdd.map(_ * 2)
    // collect不会转换RDD，会触发作业的执行
    // 所以将collect这样的方法称为 行动(action)算子
    val ints: Array[Int] = rdd1.collect()

//    println(rdd1.collect().mkString(","))


    sc.stop()
  }

}
