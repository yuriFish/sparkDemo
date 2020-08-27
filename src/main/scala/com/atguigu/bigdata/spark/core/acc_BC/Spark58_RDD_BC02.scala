package com.atguigu.bigdata.spark.core.acc_BC

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark58_RDD_BC02 {

  def main(args: Array[String]): Unit = {
    // TODO Spark 广播变量: 分布式共享只读变量
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("acc")
    val sc = new SparkContext(sparkConf)

    val rdd1 = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
    val rdd2 = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
    val list = List(("a", 4), ("b", 5), ("c", 6))

    // join 有笛卡尔乘积效果，数据量会急剧增多。如果有shuffle操作，那么性能会非常低
//    val joinRDD: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
//    println(joinRDD.collect().mkString(","))

    val mapRDD: RDD[(String, (Int, Int))] = rdd1.map {
      case (word, count1) => {
        var count2 = 0
        for (kv <- list) {
          val w = kv._1
          val v = kv._2

          if (w == word) count2 = v
        }
        (word, (count1, count2))
      }
    }

    println(mapRDD.collect().mkString(","))

    sc.stop()
  }


}
