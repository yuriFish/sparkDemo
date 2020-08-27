package com.atguigu.bigdata.spark.core.rdd.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark17_RDD_FlatMapTest {

  def main(args: Array[String]): Unit = {

    // TODO Spark - RDD - 算子(方法)
    //    flatMap 将处理的数据进行扁平化后再进行映射处理，所以也称之为扁平映射

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val dataRDD: RDD[Any] = sc.makeRDD(List(List(1, 4, 2), 7, List(5, 3, 6)))

    val rdd: RDD[Any] = dataRDD.flatMap(
      data => {
        data match {
          case list: List[_] => list
          case d => List(d)
        }
      }
    )

    println(rdd.collect().mkString(","))

    sc.stop()
  }

}
