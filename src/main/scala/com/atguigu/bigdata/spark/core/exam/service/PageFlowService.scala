package com.atguigu.bigdata.spark.core.exam.service

import com.atguigu.bigdata.spark.core.exam.bean
import com.atguigu.bigdata.spark.core.exam.dao.PageFlowDAO
import com.atguigu.summer.framework.core.TService
import com.atguigu.summer.framework.util.EnvUtil
import org.apache.spark.rdd.RDD


class PageFlowService extends TService{

  private val pageFlowDAO = new PageFlowDAO

  override def analysis() = {

    // TODO 功能补充: 对指定页面流程进行页面单跳转换率的统计
    val flowIds = List(1,2,3,4,5,6,7)
    // tail list去掉第一个值
    val okFlowIds: List[String] = flowIds.zip(flowIds.tail).map(
      t => {
        t._1 + "-" + t._2
      }
    )

    //  TODO 获取用户行为的数据
    val actionRDD: RDD[bean.UserVisitAction] = pageFlowDAO.getUserVistAction()
    actionRDD.cache()
    //    println("数据量:" + actionRDD.count())

    //  TODO 计算分母
    //   将数据进行过滤后进行统计
    val filterRDD: RDD[bean.UserVisitAction] = actionRDD.filter(
      action => {
        flowIds.init.contains(action.page_id.toInt)   // init list去掉最后一个值
      }
    )
    val pageToOneRDD: RDD[(Long, Int)] = filterRDD.map(
      action => {
        (action.page_id, 1)
      }
    )
    val pageToSumRDD: RDD[(Long, Int)] = pageToOneRDD.reduceByKey(_ + _)
    val pageCountArray: Array[(Long, Int)] = pageToSumRDD.collect()

    //  TODO 计算分子

    //    TODO 将数据根据用户Session进行分组
    val sessionRDD: RDD[(String, Iterable[bean.UserVisitAction])] = actionRDD.groupBy(_.session_id)

    val pageFlowRDD: RDD[(String, List[(String, Int)])] = sessionRDD.mapValues(
      iter => {
        //  TODO 将分组后的数据根据时间进行排序
        val actions: List[bean.UserVisitAction] = iter.toList.sortWith(
          (left, right) => {
            left.action_time < right.action_time
          }
        )

        //  TODO 将排序后的数据进行结构的转换   action => page_id
        val pageIds: List[Long] = actions.map(_.page_id)
        // 这里有严重的逻辑错误， 比如1-9-2-10-3，经过过滤后，会被认为是1-2-3，应该在格式转换后进行过滤
//        pageIds.filter(
//          id => {
//            flowIds.contains(id.toInt)
//          }
//        )

        //  TODO 将转换后的结果进行格式的转换   1,2,3,4 => (1,2), (2,3), (3,4) => (1-2, 1), (2-3, 1), (3-4, 1)
        // 1,2,3,4 => (1,2), (2,3), (3,4)
        val zipIds: List[(Long, Long)] = pageIds.zip(pageIds.tail)
        // (1,2), (2,3), (3,4) => (1-2, 1), (2-3, 1), (3-4, 1)
        zipIds.map {
          case (pageId_1, pageId_2) => {
            (pageId_1 + "-" + pageId_2, 1)
          }
        }.filter{
          case (ids, one) => {
            okFlowIds.contains(ids)
          }
        }
      }
    )

    //    TODO 将分组后的数据进行结构的转换
    val pageIdSumRDD: RDD[List[(String, Int)]] = pageFlowRDD.map(_._2)
    //    (1-2, 1)
    val pageFlowRDD1: RDD[(String, Int)] = pageIdSumRDD.flatMap(list=>list)
    //    (1-2, sum)
    val pageFlowToSumRDD: RDD[(String, Int)] = pageFlowRDD1.reduceByKey(_ + _)

    //  TODO 计算页面单跳转换率    1-2/1, 2-3/2
    pageFlowToSumRDD.foreach{
      case (pageFlow, sum) => {
        val pageId: String = pageFlow.split("-")(0)
        val value: Int = pageCountArray.toMap.getOrElse(pageId.toLong, 1)

        println("页面跳转[" + pageFlow + "]的转换率为" + (sum.toDouble / value * 100).formatted("%.2f") + "%")
      }
    }

  }

  def analysis2() = {

    //  TODO 获取用户行为的数据
    val actionRDD: RDD[bean.UserVisitAction] = pageFlowDAO.getUserVistAction()
    actionRDD.cache()
//    println("数据量:" + actionRDD.count())

    //  TODO 计算分母
    val pageToOneRDD: RDD[(Long, Int)] = actionRDD.map(
      action => {
        (action.page_id, 1)
      }
    )
    val pageToSumRDD: RDD[(Long, Int)] = pageToOneRDD.reduceByKey(_ + _)
    val pageCountArray: Array[(Long, Int)] = pageToSumRDD.collect()

    //  TODO 计算分子

    //    TODO 将数据根据用户Session进行分组
    val sessionRDD: RDD[(String, Iterable[bean.UserVisitAction])] = actionRDD.groupBy(_.session_id)

    val pageFlowRDD: RDD[(String, List[(String, Int)])] = sessionRDD.mapValues(
      iter => {
        //  TODO 将分组后的数据根据时间进行排序
        val actions: List[bean.UserVisitAction] = iter.toList.sortWith(
          (left, right) => {
            left.action_time < right.action_time
          }
        )

        //  TODO 将排序后的数据进行结构的转换   action => page_id
        val pageIds: List[Long] = actions.map(_.page_id)

        //  TODO 将转换后的结果进行格式的转换   1,2,3,4 => (1,2), (2,3), (3,4) => (1-2, 1), (2-3, 1), (3-4, 1)
        // 1,2,3,4 => (1,2), (2,3), (3,4)
        val zipIds: List[(Long, Long)] = pageIds.zip(pageIds.tail)
        // (1,2), (2,3), (3,4) => (1-2, 1), (2-3, 1), (3-4, 1)
        zipIds.map {
          case (pageId_1, pageId_2) => {
            (pageId_1 + "-" + pageId_2, 1)
          }
        }
      }
    )

    //    TODO 将分组后的数据进行结构的转换
    val pageIdSumRDD: RDD[List[(String, Int)]] = pageFlowRDD.map(_._2)
    //    (1-2, 1)
    val pageFlowRDD1: RDD[(String, Int)] = pageIdSumRDD.flatMap(list=>list)
    //    (1-2, sum)
    val pageFlowToSumRDD: RDD[(String, Int)] = pageFlowRDD1.reduceByKey(_ + _)

    //  TODO 计算页面单跳转换率    1-2/1, 2-3/2
    pageFlowToSumRDD.foreach{
      case (pageFlow, sum) => {
        val pageId: String = pageFlow.split("-")(0)
        val value: Int = pageCountArray.toMap.getOrElse(pageId.toLong, 1)

        println("页面跳转[" + pageFlow + "]的转换率为" + (sum.toDouble / value * 100).formatted("%.2f") + "%")
      }
    }

  }
}
