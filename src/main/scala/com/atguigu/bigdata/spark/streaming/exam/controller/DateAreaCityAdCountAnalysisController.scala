package com.atguigu.bigdata.spark.streaming.exam.controller

import com.atguigu.bigdata.spark.streaming.exam.service.DateAreaCityAdCountAnalysisService
import com.atguigu.summer.framework.core.TController

class DateAreaCityAdCountAnalysisController extends TController{

  private val dateAreaCityAdCountAnalysisService = new DateAreaCityAdCountAnalysisService

  override def executor(): Unit = {
    dateAreaCityAdCountAnalysisService.analysis()
  }
}
