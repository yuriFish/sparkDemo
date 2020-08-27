package com.atguigu.bigdata.spark.core.rdd.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark20_RDD_Filter {

  def main(args: Array[String]): Unit = {

    // TODO Spark - RDD - 算子(方法)
    //    filter 将数据根据指定的规则进行筛选过滤，符合规则的数据保留，不符合规则的数据丢弃。
    //            当数据进行筛选过滤后，分区不变，但是分区内的数据可能不均衡，生产环境下，可能会出现数据倾斜。

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 4, 2, 7, 5, 3, 6), 3)

    // TODO 过滤
//    val rdd1: RDD[Int] = dataRDD.filter(
//      num => {
//        num % 2 == 0
//      }
//    )
//    rdd1.collect().foreach(println)

    // TODO  小功能: 从服务器日志数据apache.log中获取2015年5月17日的请求路径

    val func1_RDD1: RDD[String] = sc.textFile("input/apache.log")
    val func1_RDD2: RDD[(String, String)] = func1_RDD1.map(
      data => {
        (data.split(" ")(3), data.split(" ")(6))
      }
    )
    val func1_RDD3: RDD[(String, String)] = func1_RDD2.filter(
      lines => {
        lines._1.substring(0, 10) == "17/05/2015"
      }
    )
//    func1_RDD3.collect().foreach(println)

    val func1_RDD4: RDD[String] = func1_RDD3.map(
      data => {
        data._2
      }
    )
    func1_RDD4.collect().foreach(println)

    sc.stop()
  }

}
