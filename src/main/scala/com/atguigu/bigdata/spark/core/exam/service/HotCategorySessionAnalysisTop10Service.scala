package com.atguigu.bigdata.spark.core.exam.service


import com.atguigu.bigdata.spark.core.exam.bean
import com.atguigu.bigdata.spark.core.exam.dao.HotCategorySessionAnalysisTop10DAO
import com.atguigu.summer.framework.core.TService
import com.atguigu.summer.framework.util.EnvUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable


class HotCategorySessionAnalysisTop10Service extends TService{

  private val hotCategorySessionAnalysisTop10DAO = new HotCategorySessionAnalysisTop10DAO

  override def analysis(data: Any) = {
    val top10: List[bean.HotCategory] = data.asInstanceOf[List[bean.HotCategory]]
    val top10Ids: List[String] = top10.map(_.categoryId)

    // TODO 使用广播变量实现数据的传播   优化
    val bcList: Broadcast[List[String]] = EnvUtil.getEnv().broadcast(top10Ids)

    // TODO 获取用户行为的数据
    val actionRDD: RDD[bean.UserVisitAction] = hotCategorySessionAnalysisTop10DAO.getUserVistAction("input/user_visit_action.txt")

//    println("before filter = " + actionRDD.count())
    // TODO 对数据进行过滤
    // 对用户的点击行为进行过滤,
    val filterRDD: RDD[bean.UserVisitAction] = actionRDD.filter(
      action => {
        if (action.click_category_id != -1) {
//          top10Ids.contains(action.click_category_id.toString)
          val flag: Boolean = bcList.value.contains(action.click_category_id.toString)
          flag
//          var flag = false
//          top10.foreach(
//            hc => {
//              if (hc.categoryId == action.click_category_id.toString) flag = true
//            }
//          )
//          flag
        } else {
          false
        }
      }
    )
//    println("after filter = " + filterRDD.count())

    // TODO 将过滤后的数据进行处理 (品类_Session, 1) => (品类_Session, sum)
    val mapRDD1: RDD[(String, Int)] = filterRDD.map(
      action => {
        (action.click_category_id + "_" + action.session_id, 1)
      }
    )
    val reduceRDD: RDD[(String, Int)] = mapRDD1.reduceByKey(_ + _)

    // TODO 将统计后的结果进行结构转换 (品类_Session, sum) => (品类, (Session, sum))
    val mapRDD2: RDD[(String, (String, Int))] = reduceRDD.map {
      case (key, count) => {
        val ks: Array[String] = key.split("_")
        (ks(0), (ks(1), count))
      }
    }

    // TODO 将转换结构后的数据对品类进行分组 (品类, (Session, sum)) => (品类, Iterator[(Session1, sum1), (Session2, sum2)])
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD2.groupByKey()

    // TODO 将分组后的数据进行排序，取前10名
    val resultRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
      iter => {
        iter.toList.sortWith(
          (left, right) => {
            left._2 > right._2
          }
        ).take(10)
      }
    )
    resultRDD.collect()

  }
}
