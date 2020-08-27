package com.atguigu.bigdata.spark.core.exam.controller

import com.atguigu.bigdata.spark.core.exam.service.PageFlowService
import com.atguigu.summer.framework.core.TController

class PageFlowController extends TController{

  private val pageFlowService = new PageFlowService

  override def executor(): Unit = {
    pageFlowService.analysis()
//    result.foreach(println)
  }
}
