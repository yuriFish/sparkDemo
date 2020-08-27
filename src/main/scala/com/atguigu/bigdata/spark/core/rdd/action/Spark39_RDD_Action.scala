package com.atguigu.bigdata.spark.core.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark39_RDD_Action {

  def main(args: Array[String]): Unit = {

    // TODO Spark - RDD - 算子 - 行动
    //    所谓的行动算子，其实不会产生新的RDD，而是触发作业的执行
    //    行动算子执行后，会获得作业的执行结果
    //    转换算子不会触发作业的执行，只是功能的扩展和包装

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Action")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))

    // TODO reduce 聚集RDD中的所有元素，先聚合分区内数据，再聚合分区间数据
    val reduceResult: Int = rdd.reduce(_+_)
    println("reduceResult = " + reduceResult)

    // TODO collect 采集数据, 以数组Array的形式返回数据集的所有元素
    //  collect 会将所有分区计算的结果拉到当前节点的内存中，可能会出现内存溢出
    val collectResult: Array[Int] = rdd.collect()
    println("collectResult = " + collectResult)

    // TODO count 返回RDD中元素的个数
    val countResult: Long = rdd.count()
    println("countResult = " + countResult)

    // TODO first 返回RDD中的第一个元素
    val firstResult: Int = rdd.first()
    println("firstResult = " + firstResult)

    // TODO take 返回一个由RDD的前n个元素组成的数组
    val takeResult: Array[Int] = rdd.take(3)
    println("takeResult = " + takeResult.mkString(","))


    sc.stop()
  }

}
