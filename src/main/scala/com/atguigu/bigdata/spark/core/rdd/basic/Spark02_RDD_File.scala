package com.atguigu.bigdata.spark.core.rdd.basic

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("wordCount")
    val sc = new SparkContext(sparkConf)

    // TODO Spark - 从磁盘(File)中创建RDD
    // path: 读取文件(目录)的路径
    // path可以设定相对路径，如果是IDEA，那么相对路径从项目的根开始查找
    // path路径根据环境的路径自动发生改变

    // 读取文件时，默认采用Hadoop读取文件的规则
    // 默认一行一行的读取内容
    // val fileRDD: RDD[String] = sc.textFile("input/")
    val fileRDD: RDD[String] = sc.textFile("input/word*.txt")
    println(fileRDD.collect().mkString(","))



    sc.stop()
  }

}
