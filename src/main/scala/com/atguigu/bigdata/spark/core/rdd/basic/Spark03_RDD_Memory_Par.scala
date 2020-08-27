package com.atguigu.bigdata.spark.core.rdd.basic

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Memory_Par {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("wordCount")
    val sc = new SparkContext(sparkConf)

    // TODO Spark - 从内存中创建RDD
    // makeRDD 第一个参数: 数据源
    // makeRDD 第二个参数: numSlices Int = defaultParallelism(默认并行度)
    // RDD中的分区的数量就是并行度，设定并行度，其实就是在设定分区的数量
    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), numSlices = 2)
//    println(rdd.collect().mkString(","))
    rdd1.saveAsTextFile("output/memory_par_1/")
    // 12, 34

    val rdd2: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5), numSlices = 3)
    rdd2.saveAsTextFile("output/memory_par_2/")
    // 1, 23, 45

    val rdd3: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), numSlices = 4)
    rdd3.saveAsTextFile("output/memory_par_3/")
    // 1, 2, 3, 4

    sc.stop();

  }

}
