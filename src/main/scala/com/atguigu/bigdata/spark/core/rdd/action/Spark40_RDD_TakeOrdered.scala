package com.atguigu.bigdata.spark.core.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark40_RDD_TakeOrdered {

  def main(args: Array[String]): Unit = {

    // TODO Spark - RDD - 算子 - 行动
    //    takeOrdered 返回该RDD排序后的前n个元素组成的数组

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Action")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(3,1,2,4))

    // TODO
    val result: Array[Int] = rdd.takeOrdered(3)
    println(result.mkString(","))

    sc.stop()
  }

}
