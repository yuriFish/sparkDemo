package com.atguigu.bigdata.spark.core.requierment.service

import com.atguigu.bigdata.spark.core.requierment.application.WordCountApplication.envData
import com.atguigu.bigdata.spark.core.requierment.dao.WordCountDAO
import com.atguigu.summer.framework.core.TService
import com.atguigu.summer.framework.util.EnvUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class WordCountService extends TService{

  private val wordCountDAO = new WordCountDAO

  override def analysis() = {
//    val sc: SparkContext = envData.asInstanceOf[SparkContext]
//    val fileRDD: RDD[String] = sc.textFile("input/word*.txt")
    val fileRDD: RDD[String] = wordCountDAO.readFile("input/word*.txt")
    val wordRDD: RDD[String] = fileRDD.flatMap( _.split(" ") )
    val word2OneRDD: RDD[(String, Int)] = wordRDD.map((_,1))
    val word2CountRDD: RDD[(String, Int)] = word2OneRDD.reduceByKey(_+_)
    val word2Count: Array[(String, Int)] = word2CountRDD.collect()
    word2Count
  }
}
