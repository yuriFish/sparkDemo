package com.atguigu.bigdata.spark.core.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark43_RDD_CountByKey {

  def main(args: Array[String]): Unit = {

    // TODO Spark - RDD - 算子 - 行动
    //    countByKey 统计每种key的个数

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Action")
    val sc = new SparkContext(sparkConf)

    val rdd1: RDD[(Int, String)] = sc.makeRDD(List((1, "a"), (1, "a"), (1, "a"), (2, "b"), (3, "c"), (3, "c")))
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 1), ("a", 1), ("b", 2), ("c", 3), ("c", 3)))

    // TODO 统计每种key的个数
    val result1: collection.Map[Int, Long] = rdd1.countByKey()
    println(result1)

    val result2: collection.Map[String, Long] = rdd2.countByKey()
    println(result2)


    sc.stop()
  }

}
