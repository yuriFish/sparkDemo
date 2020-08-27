package com.atguigu.bigdata.spark.core.rdd.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark29_RDD_ReduceByKey {

  def main(args: Array[String]): Unit = {

    // TODO Spark - RDD - 算子(方法)
    //    	Key - Value类型
    //    ReduceByKey 可以将数据按照相同的Key对Value进行聚合

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO Spark很多方法都是基于key进行操作的，所以数据格式应该为键值对(对偶元组)
    val dataRDD: RDD[(String, Int)] = sc.makeRDD(List(("hello", 1), ("spark", 1), ("hello", 2), ("scala", 1)))

    // TODO
    val rdd1 = dataRDD.reduceByKey(_+_)
    val rdd2 = dataRDD.reduceByKey(_+_, 2)

    println(rdd1.collect().mkString(","))
    println(rdd2.collect().mkString(","))

    // TODO  小功能：WordCount


    sc.stop()
  }

}
