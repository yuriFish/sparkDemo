package com.atguigu.bigdata.spark.core.requierment.controller

import com.atguigu.bigdata.spark.core.requierment.application.WordCountApplication.envData
import com.atguigu.bigdata.spark.core.requierment.service.WordCountService
import com.atguigu.summer.framework.core.TController
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class WordCountController extends TController{

  private val wordCountService = new WordCountService

  override def executor(): Unit = {
    val word2Count: Array[(String, Int)] = wordCountService.analysis()
    word2Count.foreach(println)
  }
}
