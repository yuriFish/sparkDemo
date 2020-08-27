package com.atguigu.bigdata.spark.core.rdd.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark33_RDD_CombineByKey {

  def main(args: Array[String]): Unit = {

    // TODO Spark - RDD - 算子(方法)
    //    	Key - Value类型
    //    CombineByKey 最通用的对key-value型rdd进行聚集操作的聚集函数（aggregation function）。
    //                  类似于aggregate()，combineByKey()允许用户返回值的类型与输入不一致。

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO: 求每个key的平均值: 相同key的总和 / 相同key的数量
    // 如果计算时发现相同的 key 的 value 不符合计算规则的格式的话，那么选择 combineByKey
    val dataRDD: RDD[(String, Int)] = sc.makeRDD(
      List(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)),
      2
    )

    // TODO: combineByKey 可以传递三个参数
    //  第一个参数表示： 将计算的第一个值转换结构
    //  第二个参数表示： 分区内的计算规则
    //  第三个参数表示： 分区间的计算规则
    val combineRdd: RDD[(String, (Int, Int))] = dataRDD.combineByKey(
      (_, 1), // v => {v, 1},
      (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    )

    combineRdd.collect().foreach(println)

    combineRdd.map{
      case (str, tuple) => {
        (str, tuple._1 / tuple._2)
      }
    }.collect().foreach(println)

    // TODO 思考一个问题：reduceByKey、foldByKey、aggregateByKey、combineByKey的区别？
    //  从源码角度来说，这四个算子的底层逻辑是相同的
    //  reduceByKey     不会对第一个value进行处理，分区间和分区内的计算规则相同
    //  aggregateByKey  算子会将初始值和第一个value使用分区内的计算规则进行计算
    //  foldByKey       分区内计算规则和分区间计算规则相同的 aggregateByKey
    //  combineByKey    第一个参数就是对第一个value进行处理，无需初始值

    sc.stop()
  }

}
