package com.atguigu.bigdata.spark.core.rdd.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark32_RDD_FoldByKey {

  def main(args: Array[String]): Unit = {

    // TODO Spark - RDD - 算子(方法)
    //    	Key - Value类型
    //    FoldByKey 当分区内计算规则和分区间计算规则相同时，aggregateByKey就可以简化为foldByKey

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO: 当分区内计算规则和分区间计算规则相同时，aggregateByKey就可以简化为foldByKey
    val dataRDD1: RDD[(String, Int)] = sc.makeRDD(
      List(
        ("hello", 1), ("hello", 1), ("scala", 1),
        ("hello", 1), ("hello", 2), ("scala", 3)
      ), 2
    )
    val resultRDD1: RDD[(String, Int)] = dataRDD1.foldByKey(2)(_+_)
    resultRDD1.collect().foreach(println)


    sc.stop()
  }

}
