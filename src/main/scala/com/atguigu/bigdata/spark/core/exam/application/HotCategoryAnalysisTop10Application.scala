package com.atguigu.bigdata.spark.core.exam.application

import java.util.Date

import com.atguigu.bigdata.spark.core.exam.controller.HotCategoryAnalysisTop10Controller
import com.atguigu.summer.framework.core.TApplication

object HotCategoryAnalysisTop10Application extends App with TApplication{

  private val start_time: Long = new Date().getTime
  // 热门品类前10应用程序
  start("spark") {
    val controller = new HotCategoryAnalysisTop10Controller
    controller.executor()

  }
  private val end_time: Long = new Date().getTime

  println("运行时间:" + (end_time-start_time) / 1000.0 + "s")
}
