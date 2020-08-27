package com.atguigu.bigdata.spark.core.rdd.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark23_RDD_Coalesce {

  def main(args: Array[String]): Unit = {

    // TODO Spark - RDD - 算子(方法)
    //    coalesce 根据数据量缩减分区，用于大数据集过滤后，提高小数据集的执行效率
    //              当spark程序中，存在过多的小任务的时候，可以通过coalesce方法，收缩合并分区，减少分区的个数，减小任务调度成本

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val dataRDD: RDD[Int] = sc.makeRDD(List(1,1,1,2,2,2), 4)

    // TODO 当数据过滤后，发现数据不够均匀，那么可以缩减分区
    //      如果发现数据分区不合理，也可以直接缩减分区
    val dataRDD1 = dataRDD.filter( num => num % 2 == 0 )
    dataRDD1.saveAsTextFile("output/coalesce_1/")

    // coalesce主要目的是缩减分区，扩大分区时没有效果
    // 为什么不能扩大分区，因为底层的shuffle=false，所以在分区缩减时，不会打乱数据重写组合
    // 第一个参数: 缩减后分区的数量
    // 第二个参数: shuffle值，默认为false，设置为true后，等价于repartition
    val dataRDD2 = dataRDD1.coalesce(1)
    dataRDD2.saveAsTextFile("output/coalesce_2/")

    // TODO 思考一个问题：我想要扩大分区，怎么办？

    sc.stop()
  }

}
