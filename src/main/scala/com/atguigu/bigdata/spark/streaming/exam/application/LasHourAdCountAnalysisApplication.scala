package com.atguigu.bigdata.spark.streaming.exam.application

import com.atguigu.bigdata.spark.streaming.exam.controller.LasHourAdCountAnalysisController
import com.atguigu.summer.framework.core.TApplication

object LasHourAdCountAnalysisApplication extends App with TApplication{

  start("sparkStreaming") {
    val controller = new LasHourAdCountAnalysisController
    controller.executor()
  }
}
