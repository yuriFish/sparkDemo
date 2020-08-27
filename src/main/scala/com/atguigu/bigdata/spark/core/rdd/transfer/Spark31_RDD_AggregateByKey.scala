package com.atguigu.bigdata.spark.core.rdd.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark31_RDD_AggregateByKey {

  def main(args: Array[String]): Unit = {

    // TODO Spark - RDD - 算子(方法)
    //    	Key - Value类型
    //    aggregateByKey 将数据根据不同的规则进行分区内计算和分区间计算

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO : 取出每个分区内相同key的最大值然后分区间相加
    // aggregateByKey算子是函数柯里化，存在两个参数列表
    // 1. 第一个参数列表中的参数表示初始值
    //    用于在分区内进行计算时，当做初始值使用
    // 2. 第二个参数列表中含有两个参数
    //    2.1 第一个参数表示分区内的计算规则
    //    2.2 第二个参数表示分区间的计算规则
    val dataRDD1: RDD[(String, Int)] = sc.makeRDD(
      List(
        ("hello", 1), ("hello", 1), ("scala", 1),
        ("hello", 1), ("hello", 2), ("scala", 3)
      ), 2
    )
    val resultRDD1: RDD[(String, Int)] = dataRDD1.aggregateByKey(2)(_+_, _+_)
    resultRDD1.collect().foreach(println)


    val dataRDD2 = sc.makeRDD(
      List(
        ("a",1),("a",2),("c",3),
        ("b",4),("c",5),("c",6)
      ),2
    )
    // 0:("a",1),("a",2),("c",3) => (a,3)(c,3)
    //                                         => (a,3)(b,4)(c,9)
    // 1:("b",4),("c",5),("c",6) => (b,4)(c,6)
    val resultRDD2 =
      dataRDD2.aggregateByKey(3)(
        (x, y) => math.max(x, y),
        (x, y) => x + y
      )
    resultRDD2.collect().foreach(println)

    sc.stop()
  }

}
