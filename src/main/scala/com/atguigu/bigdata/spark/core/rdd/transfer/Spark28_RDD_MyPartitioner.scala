package com.atguigu.bigdata.spark.core.rdd.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

object Spark28_RDD_MyPartitioner {

  def main(args: Array[String]): Unit = {

    // TODO Spark - RDD - 算子(方法)
    //    	Key - Value类型

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 自定义分区器 - 自己决定数据放置在哪个分区做处理
    // cba, nba, wnba
    val dataRDD: RDD[(String, String)] = sc.makeRDD(
      List(
        ("cba", "消息1"), ("cba", "消息2"), ("cba", "消息3"),
        ("nba", "消息4"), ("wnba", "消息5"), ("nba", "消息6")
      ),
      1
    )

    val rdd1: RDD[(String, String)] = dataRDD.partitionBy(new MyPartitioner(3))

    val rdd2: RDD[(Int, (String, String))] = rdd1.mapPartitionsWithIndex(
      (index, datas) => {
        datas.map(
          data => (index, data)
        )
      }
    )

    rdd2.collect().foreach(println)

    sc.stop()
  }

  // TODO 自定义分区器
  // 1. 和Partitioner发生关联, extends Partitioner
  class MyPartitioner(num: Int) extends Partitioner {

    // 获取分区的数量
    override def numPartitions: Int = {
      num
    }

    // 根据Key来决定数据在哪个分区中处理
    // 方法的返回值表示分区编号(索引)
    override def getPartition(key: Any): Int = {
      key match {
        case "nba" => 0
        case _ => 1
      }
    }
  }

}
