package com.atguigu.bigdata.spark.core.exam.dao

import com.atguigu.bigdata.spark.core.exam.bean.UserVisitAction
import com.atguigu.summer.framework.core.TDAO
import org.apache.spark.rdd.RDD

class HotCategorySessionAnalysisTop10DAO extends TDAO {

  def getUserVistAction(path:String = "input/user_visit_action.txt") = {
    val rdd: RDD[String] = readFile(path)
    rdd.map(
      line => {
        val datas: Array[String] = line.split("_")
        UserVisitAction(
          datas(0),
          datas(1).toLong,
          datas(2),
          datas(3).toLong,
          datas(4),
          datas(5),
          datas(6).toLong,
          datas(7).toLong,
          datas(8),
          datas(9),
          datas(10),
          datas(11),
          datas(12).toLong
        )
      }
    )
  }
}
