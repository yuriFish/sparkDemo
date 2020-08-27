package com.atguigu.bigdata.spark.core.rdd.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark26_RDD_2Value {

  def main(args: Array[String]): Unit = {

    // TODO Spark - RDD - 算子(方法)
    //    双Value类型


    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val dataRDD1: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6), 2)
    val dataRDD2: RDD[Int] = sc.makeRDD(List(3,6,5,4,7,8), 2)

    // TODO 并集 union 对源RDD和参数RDD求并集后返回一个新的RDD
    // union 数据合并，分区也会合并
    val unionRDD = dataRDD1.union(dataRDD2)
    println(unionRDD.collect().mkString(","))
    println("unionRDD partition num :" + unionRDD.partitions.length)
    unionRDD.saveAsTextFile("output/2Value_union/")
    // 1,2,3,4,3,4,5,6    4个分区

    // TODO 交集 intersection 对源RDD和参数RDD求交集后返回一个新的RDD
    // intersection 保留最大的分区数，数据被打乱重组， shuffle
    val interRDD = dataRDD1.intersection(dataRDD2)
//    println(interRDD.collect().mkString(","))
//    interRDD.saveAsTextFile("output/2Value_intersection/")
    // 4,3    2个分区

    // TODO 差集 subtract 以一个RDD元素为主，去除两个RDD中重复元素，将其他元素保留下来。求差集
    // subtract 分区数量等于当前rdd数量
    val subRDD = dataRDD1.subtract(dataRDD2)
//    println(subRDD.collect().mkString(","))
//    subRDD.saveAsTextFile("output/2Value_subtract/")
    // 2,1

    // TODO 拉链 zip 将两个RDD中的元素，以键值对的形式进行合并。其中，键值对中的Key为第1个RDD中的元素，Value为第2个RDD中的元素。
    //  zip 分区一致，但数据量不同时，会发生错误：
    //          Exception: Can only zip RDDs with same number of elements in each partition
    //      分区不一致，数据量不同，但每个分区数据量一致时，会发生错误：
    //          Exception: Can't zip RDDs with unequal numbers of partitions: List(a, b)
    val zipRDD = dataRDD1.zip(dataRDD2)
//    println(zipRDD.collect().mkString(","))
//    zipRDD.saveAsTextFile("output/2Value_zip/")
    // (1,3),(2,4),(3,5),(4,6)

    // TODO 拉链 zip 当zip两个List集合时，数量不一致也可以进行zip
//    val data1: List[Int] = List(1,2,3,4,5,6,7)
//    val data2: List[Int] = List(2,3,4,5,6,7)
//    val zipRDD1 = data1.zip(data2)
//    println(zipRDD1.mkString(","))

    // TODO 思考一个问题：如果两个RDD数据类型不一致怎么办？
    //  交集，并集，差集，会发生错误，但是可以进行zip

    sc.stop()
  }

}
