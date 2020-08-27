package com.atguigu.bigdata.spark.streaming.exam.controller

import com.atguigu.bigdata.spark.streaming.exam.service.MockDataService
import com.atguigu.summer.framework.core.TController

class MockDataController extends TController{

  private val mockDataService = new MockDataService

  override def executor() = {
    val result = mockDataService.analysis()
  }
}
