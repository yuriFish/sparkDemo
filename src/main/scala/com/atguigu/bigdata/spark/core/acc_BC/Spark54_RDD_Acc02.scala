package com.atguigu.bigdata.spark.core.acc_BC

import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Spark54_RDD_Acc02 {

  def main(args: Array[String]): Unit = {
    //  TODO Spark acc 累加器
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("acc")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(("a",1), ("a",2), ("a",3)))

//    val sum = rdd.reduceByKey(_ + _)
//    println("sum=" + sum)
//    var sum = 0   // 这个得出的结果，还是(a, 0)

    // TODO 声明累加器变量
    val sum: LongAccumulator = sc.longAccumulator("sum")
    // TODO 使用累加器完成数据的累加
    rdd.foreach{
      case (word, count) => {
        sum.add(count)
        println("sum=" + sum)
      }
    }
    println("(a, " + sum.value + ")")

    sc.stop()
  }

}
