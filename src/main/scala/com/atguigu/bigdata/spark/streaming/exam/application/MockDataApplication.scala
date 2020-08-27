package com.atguigu.bigdata.spark.streaming.exam.application

import com.atguigu.bigdata.spark.streaming.exam.controller.MockDataController
import com.atguigu.summer.framework.core.TApplication

object MockDataApplication extends App with TApplication{

  start("sparkStreaming") {
    val controller = new MockDataController
    controller.executor()
  }
}
