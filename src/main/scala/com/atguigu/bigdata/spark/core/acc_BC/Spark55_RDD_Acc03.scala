package com.atguigu.bigdata.spark.core.acc_BC

import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Spark55_RDD_Acc03 {

  def main(args: Array[String]): Unit = {
    // TODO Spark acc 累加器: 分布式共享只写变量
    //   1. 将累加器变量注册到spark中
    //   2. 执行计算时，spark会将累加器发送到executor执行计算
    //   3. 计算完毕后，executor会将累加器的计算结果返回到driver端
    //   4. driver端获取到多个累计器的结果，然后两两合并，最后得到累加器的执行结果
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("acc")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(("a",1), ("a",2), ("a",3)))

    // TODO 声明累加器变量
    val sum: LongAccumulator = sc.longAccumulator("sum")
//    sc.doubleAccumulator()
//    sc.collectionAccumulator()


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
