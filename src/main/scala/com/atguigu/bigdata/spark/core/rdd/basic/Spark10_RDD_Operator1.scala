package com.atguigu.bigdata.spark.core.rdd.basic

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark10_RDD_Operator1 {

  def main(args: Array[String]): Unit = {

    // TODO Spark - RDD - 算子(方法)

    val sparkConf = new SparkConf().setMaster("local").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    // TODO 分区问题
    // RDD有分区列表
    // 默认分区数量不变，数据会转换后输出
    val rdd1: RDD[Int] = rdd.map(_ * 2)

    rdd1.saveAsTextFile("output/operator1/")


    sc.stop()
  }

}
