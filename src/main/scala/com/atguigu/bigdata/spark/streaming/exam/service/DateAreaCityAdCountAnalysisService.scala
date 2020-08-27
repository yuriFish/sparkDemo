package com.atguigu.bigdata.spark.streaming.exam.service

import java.sql.{Connection, PreparedStatement}
import java.text.SimpleDateFormat

import com.atguigu.bigdata.spark.streaming.exam.bean.Ad_Click_Log
import com.atguigu.bigdata.spark.streaming.exam.dao.DateAreaCityAdCountAnalysisDAO
import com.atguigu.summer.framework.core.TService
import com.atguigu.summer.framework.util.JDBCUtil
import org.apache.spark.streaming.dstream.DStream

class DateAreaCityAdCountAnalysisService extends TService{

  private val dateAreaCityAdCountAnalysisDAO = new DateAreaCityAdCountAnalysisDAO

  /**
   * 数据分析
   *
   * @return
   */
  override def analysis() = {

    // TODO 读取kafka的数据
    val messageDS: DStream[String] = dateAreaCityAdCountAnalysisDAO.readKafka()

    // TODO 将数据转换为样例类来使用
    val logDS: DStream[Ad_Click_Log] = messageDS.map(
      data => {
        val datas = data.split(" ")
        Ad_Click_Log(datas(0), datas(1), datas(2), datas(3), datas(4))
      }
    )

    // TODO 对数据进行统计
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val dayDS: DStream[((String, String, String, String), Int)] = logDS.map(
      log => {
        val ts = log.ts
        val day: String = sdf.format(new java.util.Date(ts.toLong))
        ((day, log.area, log.city, log.userid), 1)
      }
    )
    val resultDS: DStream[((String, String, String, String), Int)] = dayDS.reduceByKey(_ + _)

    // TODO 将统计结果保存到MySQL中
    resultDS.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          datas => {
            // TODO 获取数据库的连接
            val conn: Connection = JDBCUtil.getConnection()
            val pstat: PreparedStatement = conn.prepareStatement(
              """
                |insert into area_city_ad_count(dt, area, city, adid, count)
                |values (?, ?, ?, ?, ?)
                |on duplicate key
                |update count = count+?
                |""".stripMargin)

            // TODO 操作数据库

            datas.foreach {
              case ((day, area, city, adid), count) => {
                pstat.setString(1, day)
                pstat.setString(2, area)
                pstat.setString(3, city)
                pstat.setString(4, adid)
                pstat.setLong(5, count)
                pstat.setLong(6, count)
                pstat.executeUpdate()
              }

            }

            // TODO 关闭资源
            pstat.close()
            conn.close()
          }
        )
      }
    )


  }
}
