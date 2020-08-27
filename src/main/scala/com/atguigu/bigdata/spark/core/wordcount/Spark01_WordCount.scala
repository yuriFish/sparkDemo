package com.atguigu.bigdata.spark.core.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_WordCount {

  def main(args: Array[String]): Unit = {
    // TODO 准备Spark环境
    // 创建Spark运行配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("wordCount")

    // TODO 建立和Spark的连接
    // 创建Spark上下文环境对象（连接对象）
    val sc = new SparkContext(sparkConf)

    // TODO 实现业务操作

    // 读取文件数据
    val fileRDD: RDD[String] = sc.textFile("input/word*.txt")

    // 将文件中的数据进行分词
    val wordRDD: RDD[String] = fileRDD.flatMap( _.split(" ") )

    // 转换数据结构 word => (word, 1)
    // val word2OneRDD: RDD[(String, Int)] = wordRDD.map(word => (word, 1))
    val word2OneRDD: RDD[(String, Int)] = wordRDD.map((_,1))

    // 将转换结构后的数据按照相同的单词进行分组聚合
    val word2CountRDD: RDD[(String, Int)] = word2OneRDD.reduceByKey(_+_)

    // 将数据聚合结果采集到内存中
    val word2Count: Array[(String, Int)] = word2CountRDD.collect()

    // 打印结果
    word2Count.foreach(println)


    // TODO 释放连接
    sc.stop();

  }

}
