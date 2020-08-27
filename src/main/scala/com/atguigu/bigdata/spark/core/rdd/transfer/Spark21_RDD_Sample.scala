package com.atguigu.bigdata.spark.core.rdd.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark21_RDD_Sample {

  def main(args: Array[String]): Unit = {

    // TODO Spark - RDD - 算子(方法)
    //    sample 根据指定的规则从数据集中抽取数据

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 4, 2, 7, 5, 3, 6))

    // 抽取数据不放回（伯努利算法）
    // 伯努利算法：又叫0、1分布。例如扔硬币，要么正面，要么反面。
    // 具体实现：根据种子和随机算法算出一个数和第二个参数设置几率比较，小于第二个参数要，大于不要
    // 第一个参数：抽取的数据是否放回，false：不放回
    // 第二个参数：抽取的几率，范围在[0,1]之间,0：全不取；1：全取；
    // 第三个参数：随机数种子, 可以确定数据的抽取
    //            随机数不随机，所谓的随机数依靠算法实现
    val dataRDD1 = dataRDD.sample(false, 0.5)
    println(dataRDD1.collect().mkString(","))

    // 抽取数据放回（泊松算法）
    // 第一个参数：抽取的数据是否放回，true：放回；false：不放回
    // 第二个参数：重复数据的几率，范围大于等于0.表示每一个元素被期望抽取到的次数
    // 第三个参数：随机数种子
    val dataRDD2 = dataRDD.sample(true, 2)
    println(dataRDD2.collect().mkString(","))

    // TODO 思考一个问题：有啥用，抽奖吗？
    //    在实际开发中，往往会出现数据倾斜的情况，那么可以从数据倾斜的分区中抽取数据，查看
    //    数据的规则，分析后，可以进行改善处理，让数据更加均匀

    sc.stop()
  }

}
