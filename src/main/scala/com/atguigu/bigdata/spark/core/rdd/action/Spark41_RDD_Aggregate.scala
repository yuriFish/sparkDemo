package com.atguigu.bigdata.spark.core.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark41_RDD_Aggregate {

  def main(args: Array[String]): Unit = {

    // TODO Spark - RDD - 算子 - 行动
    //    aggregate 分区的数据通过初始值和分区内的数据进行聚合，然后再和初始值进行分区间的数据聚合

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Action")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 8)

    // 将该RDD所有元素相加得到结果
    val result1: Int = rdd.aggregate(0)(_ + _, _ + _)

    // TODO aggregate
    //  aggregateByKey 初始值只参与分区内的计算
    //  aggregate      初始值分区内和和分区间的计算都会参与
    val result2: Int = rdd.aggregate(10)(_ + _, _ + _)
    println(result1)
    println(result2)

    sc.stop()
  }

}
