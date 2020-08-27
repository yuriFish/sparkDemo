package com.atguigu.bigdata.spark.core.exam.service

import com.atguigu.bigdata.spark.core.exam.bean
import com.atguigu.bigdata.spark.core.exam.bean.HotCategory
import com.atguigu.bigdata.spark.core.exam.dao.HotCategoryAnalysisTop10DAO
import com.atguigu.bigdata.spark.core.exam.helper.HotCategoryAccumulator
import com.atguigu.summer.framework.core.TService
import com.atguigu.summer.framework.util.EnvUtil
import org.apache.spark.rdd.RDD

import scala.collection.mutable


class HotCategoryAnalysisTop10Service extends TService{

  private val hotCategoryAnalysisTop10DAO = new HotCategoryAnalysisTop10DAO

  override def analysis() = {
    // TODO 读取电商日志数据
    val actionRDD: RDD[String] = hotCategoryAnalysisTop10DAO.readFile("input/user_visit_action.txt")

    // TODO 对品类id进行点击的统计 (category, clickCount)
    //  使用累加器对数据进行聚合
    val accumulator = new HotCategoryAccumulator
    EnvUtil.getEnv().register(accumulator, "hotCategory")

    // TODO 将数据循环，向累加器中放
    actionRDD.foreach{
      action => {
        val datas: Array[String] = action.split("_")
        if (datas(6) != "-1") {
          accumulator.add((datas(6), "click"))
        } else if (datas(8) != "-1") {
          val ids: Array[String] = datas(8).split(",")
          ids.foreach{
            id => {
              accumulator.add((id, "order"))
            }
          }
        } else if (datas(10) != "-1") {
          val ids: Array[String] = datas(10).split(",")
          ids.foreach{
            id => {
              accumulator.add((id, "pay"))
            }
          }
        }
      }
    }

    // TODO 获取累加器的值
    val accValue: mutable.Map[String, HotCategory] = accumulator.value
    val categories: mutable.Iterable[HotCategory] = accValue.map(_._2)

    categories.toList.sortWith(
      (leftHC, rightHC) => {
        if (leftHC.clickCount > rightHC.clickCount) {
          true
        } else if (leftHC.clickCount == rightHC.clickCount) {
          if (leftHC.orderCount > rightHC.orderCount) {
            true
          } else if (leftHC.orderCount == rightHC.orderCount) {
            leftHC.payCount > rightHC.payCount
          } else {
            false
          }
        } else {
          false
        }
      }
    ).take(10)

  }

  def analysis4() = {
    // TODO 读取电商日志数据
    val actionRDD: RDD[String] = hotCategoryAnalysisTop10DAO.readFile("input/user_visit_action.txt")

    // TODO 对品类id进行点击的统计 (category, clickCount)   先过滤会好一点？结果差不多，可能是数据量小？18万行数据
    // line =>
    //       click = HotCategory(1, 0, 0)
    //       order = HotCategory(0, 1, 0)
    //       pay   = HotCategory(0, 0, 1)
    
    val flatMapRDD = actionRDD.flatMap(
      action => {
        val datas = action.split("_")
        if (datas(6) != "-1") {
          List((datas(6), HotCategory(datas(6), 1, 0, 0)))
        } else if (datas(8) != "null") {
          val ids = datas(8).split(",")
          ids.map(id => (id, HotCategory(categoryId = id, 0, 1, 0)))
        } else if (datas(10) != "null") {
          val ids = datas(10).split(",")
          ids.map(id => (id, HotCategory(categoryId = id, 0, 0, 1)))
        } else {
          Nil
        }
      }
    )

    val resultRDD = flatMapRDD.reduceByKey(
      (c1, c2) => {
        c1.clickCount = c1.clickCount + c2.clickCount
        c1.orderCount = c1.orderCount + c2.orderCount
        c1.payCount = c1.payCount + c2.payCount
        c1
      }
    )

    resultRDD.collect().sortWith(
      (left, right) => {
        val leftHC = left._2
        val rightHC = right._2
        if (leftHC.clickCount > rightHC.clickCount) {
          true
        } else if (leftHC.clickCount == rightHC.clickCount) {
          if (leftHC.orderCount > rightHC.orderCount) {
            true
          } else if (leftHC.orderCount == rightHC.orderCount) {
            leftHC.payCount > rightHC.payCount
          } else {
            false
          }
        } else {
          false
        }
      }
    ).take(10)

  }

  def analysis3() = {
    // TODO 读取电商日志数据
    val actionRDD: RDD[String] = hotCategoryAnalysisTop10DAO.readFile("input/user_visit_action.txt")
//    actionRDD.cache()  // actionRDD只用一次，就不用.cache()

    // TODO 对品类id进行点击的统计 (category, clickCount)   先过滤会好一点？结果差不多，可能是数据量小？18万行数据
    // line =>
    //       click (1, 0, 0)
    //       order (0, 1, 0)
    //       pay   (0, 0, 1)
    // 比 analysis2 要快一点 从11.2s到10s
    val flatMapRDD = actionRDD.flatMap(
      action => {
        val datas = action.split("_")
        if (datas(6) != "-1") {
          List((datas(6), (1, 0, 0)))
        } else if (datas(8) != "null") {
          val ids = datas(8).split(",")
          ids.map((_, (0, 1, 0)))
        } else if (datas(10) != "null") {
          val ids = datas(10).split(",")
          ids.map((_, (0, 0, 1)))
        } else {
          Nil
        }
      }
    )

    val resultMidRDD: RDD[(String, (Int, Int, Int))] = flatMapRDD.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._3 + t2._3, t1._3 + t2._3)
      }
    )

    // TODO 将转换结构后的数据进行排序(降序)
    val sortRDD: RDD[(String, (Int, Int, Int))] = resultMidRDD.sortBy(_._2, false)

    // TODO 将排序后的结果取前10
    val result = sortRDD.take(10)

    result
  }

  def analysis2() = {
    // TODO 读取电商日志数据
    val actionRDD: RDD[String] = hotCategoryAnalysisTop10DAO.readFile("input/user_visit_action.txt")
    actionRDD.cache() // 优化: actionRDD重复使用，可以放在缓存中

    // TODO 对品类id进行点击的统计 (category, clickCount)   先过滤会好一点？结果差不多，可能是数据量小？18万行数据
    // (品类1, 10)
//    val start_time =new Date().getTime
//    val clickRDD: RDD[(String, Int)] = actionRDD.map(
//      action => {
//        (action.split("_")(6), 1)
//      }
//    ).filter(_._1 != "-1")
    val clickRDD: RDD[(String, Int)] = actionRDD.filter(
     lines => {
       lines.split("_")(6) != "-1"
     }
    ).map(
      action => {
        (action.split("_")(6), 1)
      }
    )
//    val end_time =new Date().getTime
//    clickRDD.collect().foreach(println)
//    println("运行时间为" + (end_time-start_time))
    val categoryId2clickCountRDD: RDD[(String, Int)] = clickRDD.reduceByKey(_ + _)

    // TODO 对品类进行下单的统计 (category, orderCount)
    // 下单行为，一次可以下单多个商品，所以品类ID和产品ID可以是多个，id之间采用逗号分隔
    // (品类1, 品类2, 品类3, 10)   => (品类1, 10), (品类2, 10), (品类3, 10)
    val orderRDD: RDD[String] = actionRDD.map(
      lines => {
        lines.split("_")(8)
      }
    ).filter(_ != "null")

    val orderToOneRDD: RDD[(String, Int)] = orderRDD.flatMap {
      id => {
        val ids = id.split(",")
        ids.map((_, 1))
      }
    }

    val categoryId2orderCountRDD: RDD[(String, Int)] = orderToOneRDD.reduceByKey(_ + _)

    // TODO 对品类进行支付的统计 (category, payCount)
    val payRDD: RDD[String] = actionRDD.map(
      lines => {
        lines.split("_")(10)
      }
    ).filter(_ != "null")

    val payToOneRDD: RDD[(String, Int)] = payRDD.flatMap {
      id => {
        val ids = id.split(",")
        ids.map((_, 1))
      }
    }

    val categoryId2payCountRDD: RDD[(String, Int)] = payToOneRDD.reduceByKey(_ + _)

    // TODO 将上述的统计结果转换结构(进行聚合) (category, (clickCount, orderCount, payCount))
    // join 笛卡尔乘积很复杂，会有大量的计算，可以使用其他聚合的方法进行优化

//    val joinRDD1: RDD[(String, (Int, Int))] = categoryId2clickCountRDD.join(categoryId2orderCountRDD)
//    val joinRDD2: RDD[(String, ((Int, Int), Int))] = joinRDD1.join(categoryId2payCountRDD)
//    val resultMidRDD: RDD[(String, (Int, Int, Int))] = joinRDD2.mapValues {
//      case ((clickCount, orderCount), payCount) => {
//        (clickCount, orderCount, payCount)
//      }
//    }
    // (品类, 点击数量), (品类, 下单数量), (品类, 支付数量)
    // (品类, (点击数量, 0, 0)), (品类, (0, 下单数量, 0)), (品类, (0, 0, 支付数量))
    // (品类, (点击数量, 下单数量, 支付数量))
    val newCategoryId2clickCountRDD = categoryId2clickCountRDD.map {
      case (id, clickCount) => {
        (id, (clickCount, 0, 0))
      }
    }
    val newCategoryId2orderCountRDD = categoryId2orderCountRDD.map {
      case (id, orderCount) => {
        (id, (0, orderCount, 0))
      }
    }
    val newCategoryId2payCountRDD = categoryId2payCountRDD.map {
      case (id, payCount) => {
        (id, (0, 0, payCount))
      }
    }
    val countRDD: RDD[(String, (Int, Int, Int))] = newCategoryId2clickCountRDD.union(newCategoryId2orderCountRDD).union(newCategoryId2payCountRDD)
    val resultMidRDD: RDD[(String, (Int, Int, Int))] = countRDD.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._3 + t2._3, t1._3 + t2._3)
      }
    )

    // TODO 将转换结构后的数据进行排序(降序)
    val sortRDD: RDD[(String, (Int, Int, Int))] = resultMidRDD.sortBy(_._2, false)

    // TODO 将排序后的结果取前10
    val result = sortRDD.take(10)

    result
  }
}
