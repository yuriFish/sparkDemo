package com.atguigu.bigdata.spark.core.rdd.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark24_RDD_Repartition {

  def main(args: Array[String]): Unit = {

    // TODO Spark - RDD - 算子(方法)
    //    repartition 该操作内部其实执行的是coalesce操作，参数shuffle的默认值为true。
    //                无论是将分区数多的RDD转换为分区数少的RDD，还是将分区数少的RDD转换为分区数多的RDD，
    //                repartition操作都可以完成，因为无论如何都会经shuffle过程。

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val dataRDD: RDD[Int] = sc.makeRDD(List(1,1,1,2,2,2), 2)

    // TODO 当数据过滤后，发现数据不够均匀，那么可以缩减分区
    //      如果发现数据分区不合理，也可以直接缩减分区
    val dataRDD1 = dataRDD.filter( num => num % 2 == 0 )
    dataRDD1.saveAsTextFile("output/repartition_1/")

    val dataRDD2 = dataRDD1.repartition(4)
    dataRDD2.saveAsTextFile("output/repartition_2/")

    // TODO 思考一个问题：coalesce和repartition区别？

    sc.stop()
  }

}
