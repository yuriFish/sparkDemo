package com.atguigu.bigdata.spark.streaming.exam.controller

import com.atguigu.bigdata.spark.streaming.exam.service.LasHourAdCountAnalysisService
import com.atguigu.summer.framework.core.TController

class LasHourAdCountAnalysisController extends TController{

  private val lasHourAdCountAnalysisService = new LasHourAdCountAnalysisService

  override def executor(): Unit = {
    lasHourAdCountAnalysisService.analysis()
  }
}
