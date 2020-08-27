package com.atguigu.bigdata.spark.streaming.exam.service

import java.util.Properties

import com.atguigu.bigdata.spark.streaming.exam.dao.MockDataDAO
import com.atguigu.summer.framework.core.TService

class MockDataService extends TService{

  private val mockDataDAO = new MockDataDAO

  /**
   * 数据分析
   *
   * @return
   */
  override def analysis() = {

    // 测试生成数据 但是报错了
//    mockDataDAO.genMockData().foreach(
//      println
//    )
    val datas = mockDataDAO.genMockData _
//    println("datas.length=" + datas.length)

    // TODO 生成模拟数据
//    import mockDataDAO._

    // TODO 向Kafka中发生数据
    mockDataDAO.writeToKafka(datas)

  }
}
