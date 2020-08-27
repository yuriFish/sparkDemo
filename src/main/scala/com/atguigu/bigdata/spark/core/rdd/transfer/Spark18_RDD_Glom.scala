package com.atguigu.bigdata.spark.core.rdd.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark18_RDD_Glom {

  def main(args: Array[String]): Unit = {

    // TODO Spark - RDD - 算子(方法)
    //    glom 将同一个分区的数据直接转换为相同类型的内存数组进行处理，分区不变

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 4, 2, 7, 5, 3, 6), 3)

//    val rdd: RDD[Array[Int]] = dataRDD.glom()

//    rdd.foreach(
//      array => {
//        println(array.mkString(","))
//      }
//    )

    // TODO  小功能: 计算所有分区最大值求和（分区内取最大值，分区间最大值求和）
    val glomRDD: RDD[Array[Int]] = dataRDD.glom()

    val maxRDD: RDD[Int] = glomRDD.map(
      data => data.max
    )
    println(maxRDD.collect().mkString(","))
    println(maxRDD.sum())

    val sum: Int = maxRDD.reduce(_ + _)
    println(sum)

    sc.stop()
  }

}
