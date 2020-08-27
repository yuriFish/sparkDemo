package com.atguigu.bigdata.spark.core.acc_BC

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Spark53_RDD_Acc01 {

  def main(args: Array[String]): Unit = {
    //  TODO Spark acc 累加器
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("acc")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(("a",1), ("a",2), ("a",3)))

//    val sum = rdd.reduceByKey(_ + _)
//    println("sum=" + sum)
    var sum = 0   // 这个得出的结果，还是(a, 0)

    rdd.foreach{
      case (word, count) => {
        sum = sum + count
        println("sum=" + sum)
      }
    }
    println("(a, " + sum + ")")

    sc.stop()
  }

}
