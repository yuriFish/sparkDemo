package com.atguigu.bigdata.spark.core.exam.controller

import com.atguigu.bigdata.spark.core.exam.service.HotCategoryAnalysisTop10Service
import com.atguigu.summer.framework.core.TController


class HotCategoryAnalysisTop10Controller extends TController{

  private val hotCategoryAnalysisTop10Service = new HotCategoryAnalysisTop10Service

  override def executor(): Unit = {
    val result = hotCategoryAnalysisTop10Service.analysis()

    result.foreach(println)
  }
}
