package com.atguigu.bigdata.spark.streaming.exam.controller

import com.atguigu.bigdata.spark.streaming.exam.service.BlackListService
import com.atguigu.summer.framework.core.TController

class BlackListController extends TController{

  private val blackListService = new BlackListService

  override def executor(): Unit = {
    blackListService.analysis()
  }
}
