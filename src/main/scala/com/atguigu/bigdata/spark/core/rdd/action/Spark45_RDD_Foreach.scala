package com.atguigu.bigdata.spark.core.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark45_RDD_Foreach {

  def main(args: Array[String]): Unit = {

    // TODO Spark - RDD - 算子 - 行动
    //    foreach 分布式遍历RDD中的每一个元素，调用指定函数

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Action")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))

    // TODO foreach方法 收集后打印
    //  集合的方法中的代码是在当前节点(Driver)执行的，在节点的内存中完成
    rdd.collect().foreach(println)
    println("-----------------")
    // TODO foreach算子 分布式打印
    //  rdd的方法称为算子
    //  算子的逻辑代码是在分布式计算节点Executor执行的，在不同的计算节点完成
    //  算子之外的代码是在Driver端执行
    rdd.foreach(println)

    sc.stop()
  }

}
