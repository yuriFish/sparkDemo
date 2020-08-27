package com.atguigu.bigdata.spark.streaming.exam.application

import com.atguigu.bigdata.spark.streaming.exam.controller.DateAreaCityAdCountAnalysisController
import com.atguigu.summer.framework.core.TApplication

object DateAreaCityAdCountAnalysisApplication extends App with TApplication{

  start("sparkStreaming") {
    val controller = new DateAreaCityAdCountAnalysisController
    controller.executor()
  }
}
