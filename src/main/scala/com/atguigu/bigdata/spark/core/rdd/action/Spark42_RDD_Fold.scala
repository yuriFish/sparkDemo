package com.atguigu.bigdata.spark.core.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark42_RDD_Fold {

  def main(args: Array[String]): Unit = {

    // TODO Spark - RDD - 算子 - 行动
    //    fold 折叠操作，aggregate的简化版操作

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Action")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 8)

    // 将该RDD所有元素相加得到结果
    val result1: Int = rdd.fold(0)(_+_)

    // TODO fold
    val result2: Int = rdd.fold(10)(_+_)
    println(result1)
    println(result2)

    sc.stop()
  }

}
