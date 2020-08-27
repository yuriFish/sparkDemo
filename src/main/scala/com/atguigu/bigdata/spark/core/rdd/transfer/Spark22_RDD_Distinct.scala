package com.atguigu.bigdata.spark.core.rdd.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark22_RDD_Distinct {

  def main(args: Array[String]): Unit = {

    // TODO Spark - RDD - 算子(方法)
    //    distinct 将数据集中重复的数据去重

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 4, 2, 4, 5, 3, 2))

    //
    val dataRDD1 = dataRDD.distinct()
    println(dataRDD1.collect().mkString(","))

    val dataRDD2 = dataRDD.distinct(2)
    println(dataRDD2.collect().mkString(","))

    // TODO 思考一个问题：如果不用该算子，你有什么办法实现数据去重？
    //

    sc.stop()
  }

}
