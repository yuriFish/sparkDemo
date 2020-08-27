package com.atguigu.bigdata.spark.core.rdd.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark12_RDD_MapTest {

  def main(args: Array[String]): Unit = {

    // TODO Spark - RDD - 算子(方法)

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 从服务器日志数据 apache.log中获取用户请求URL资源路径
    val fileRDD: RDD[String] = sc.textFile("input/apache.log")

    val urlRDD: RDD[String] = fileRDD.map(lines => {
      val datas: Array[String] = lines.split(" ")
      datas(6) // 第7列数据
    })

    urlRDD.collect().foreach(println)

    sc.stop()
  }

}
