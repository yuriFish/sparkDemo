package com.atguigu.bigdata.spark.mytest

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object MyTest_08_21 {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("MyTest")
    val sc = new SparkContext(sparkConf)

    var map = mutable.Map[Int, Int]()
    map += (1 -> 1, 2 -> 1)
    println(map.keys+", "+map.values)

    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6))

    val rdd2 = rdd1.map(
      x => {
        map += (x -> 1)
        println(map.keys+", "+map.values)
        x
      }
    )

    val result = rdd2.collect()
    result.foreach(println)

    println("----->"+map.keys+", "+map.values)
  }

}
