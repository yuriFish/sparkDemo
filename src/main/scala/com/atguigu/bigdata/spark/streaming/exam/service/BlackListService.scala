package com.atguigu.bigdata.spark.streaming.exam.service

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.bigdata.spark.streaming.exam.bean.Ad_Click_Log
import com.atguigu.bigdata.spark.streaming.exam.dao.BlackListDAO
import com.atguigu.summer.framework.core.TService
import com.atguigu.summer.framework.util.JDBCUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable.ListBuffer

class BlackListService extends TService{

  private val blackListDAO = new BlackListDAO

  /**
   * 数据分析
   *
   * @return
   */
  override def analysis() = {

    // TODO 读取kafka的数据
    val ds: DStream[String] = blackListDAO.readKafka()
    //    ds.print()

    // TODO 将数据转换为样例类来使用
    val logDS: DStream[Ad_Click_Log] = ds.map(
      data => {
        val datas = data.split(" ")
        Ad_Click_Log(datas(0), datas(1), datas(2), datas(3), datas(4))
      }
    )

    // TODO 周期性地获取黑名单信息，判断当前用户的点击数据是否在黑名单中
    // Code(1)
    // Transform
    val redultDS: DStream[((String, String, String), Int)] = logDS.transform(
      rdd => {
        // Code: 周期性执行
        // TODO 读取MySQL数据库，获取黑名单信息
        val connection: Connection = JDBCUtil.getConnection()
        val pstat: PreparedStatement = connection.prepareStatement(
          """
            |select userid from black_list
            |""".stripMargin)
        val rs: ResultSet = pstat.executeQuery()
        // 黑名单的ID集合
        val blackIds = ListBuffer[String]()
        while (rs.next()) {
          blackIds.append(rs.getString(1))
        }
        rs.close()
        pstat.close()
        connection.close()

        // TODO 如果用户在黑名单中，那么将数据过滤掉，不会进行统计
        val filterRDD: RDD[Ad_Click_Log] = rdd.filter(
          log => {
            !blackIds.contains(log.userid)
          }
        )

        // TODO 将正常的数据进行点击量的统计
        // (key, 1) => (key, sum);  key => (day - uid - adid)
        // ts => day
        val sdf = new SimpleDateFormat("yyyy-MM-dd")
        val mapRDD: RDD[((String, String, String), Int)] = filterRDD.map(
          log => {
            val date = new Date(log.ts.toLong)
            //            ((log.ts, log.userid, log.adid), 1)
            ((sdf.format(date), log.userid, log.adid), 1)
          }
        )
        mapRDD.reduceByKey(_ + _)

      }
    )

    // TODO 将统计的结果中超过阈值的用户信息拉人到黑名单中
    // java.io.NotSerializableException: com.alibaba.druid.pool.DruidPooledPreparedStatement
    // TODO 数据库连接对象是无法序列化的
    redultDS.foreachRDD(
      rdd => {
        // RDD可以以分区为单位进行循环
        rdd.foreachPartition(
          datas => {
            val conn = JDBCUtil.getConnection()

            val pstat: PreparedStatement = conn.prepareStatement(
              """
                |insert into user_ad_count(dt, userid, adid, count)
                |values (?, ?, ?, ?)
                |on duplicate key
                |update count = count+?
                |""".stripMargin)

            val pstat1: PreparedStatement = conn.prepareStatement(
              """
                |insert into black_list(userid)
                |select userid from user_ad_count
                |where dt=? and userid=? and adid=? and count >= 100
                |on duplicate key
                |update userid = ?
                |""".stripMargin)

            // datas是数据，不是算子，所以不会涉及到序列化的问题
            datas.foreach {
              case ((day, userid, adid), sum) => {
                pstat.setString(1, day)
                pstat.setString(2, userid)
                pstat.setString(3, adid)
                pstat.setLong(4, sum)
                pstat.setLong(5, sum)
                pstat.executeUpdate()

                pstat1.setString(1, day)
                pstat1.setString(2, userid)
                pstat1.setString(3, adid)
                pstat1.setString(4, userid)
                pstat1.executeUpdate()

              }
            }

            pstat1.close()
            pstat.close()
            conn.close()
          }
        )

      }
    )
  }

  /**
   * 数据分析
   *
   * @return
   */
  def analysis3() = {

    // TODO 读取kafka的数据
    val ds: DStream[String] = blackListDAO.readKafka()
    //    ds.print()

    // TODO 将数据转换为样例类来使用
    val logDS: DStream[Ad_Click_Log] = ds.map(
      data => {
        val datas = data.split(" ")
        Ad_Click_Log(datas(0), datas(1), datas(2), datas(3), datas(4))
      }
    )

    // TODO 周期性地获取黑名单信息，判断当前用户的点击数据是否在黑名单中
    // Code(1)
    // Transform
    val redultDS: DStream[((String, String, String), Int)] = logDS.transform(
      rdd => {
        // Code: 周期性执行
        // TODO 读取MySQL数据库，获取黑名单信息
        val connection: Connection = JDBCUtil.getConnection()
        val pstat: PreparedStatement = connection.prepareStatement(
          """
            |select userid from black_list
            |""".stripMargin)
        val rs: ResultSet = pstat.executeQuery()
        // 黑名单的ID集合
        val blackIds = ListBuffer[String]()
        while (rs.next()) {
          blackIds.append(rs.getString(1))
        }
        rs.close()
        pstat.close()
        connection.close()

        // TODO 如果用户在黑名单中，那么将数据过滤掉，不会进行统计
        val filterRDD: RDD[Ad_Click_Log] = rdd.filter(
          log => {
            !blackIds.contains(log.userid)
          }
        )

        // TODO 将正常的数据进行点击量的统计
        // (key, 1) => (key, sum);  key => (day - uid - adid)
        // ts => day
        val sdf = new SimpleDateFormat("yyyy-MM-dd")
        val mapRDD: RDD[((String, String, String), Int)] = filterRDD.map(
          log => {
            val date = new Date(log.ts.toLong)
            //            ((log.ts, log.userid, log.adid), 1)
            ((sdf.format(date), log.userid, log.adid), 1)
          }
        )
        mapRDD.reduceByKey(_ + _)

      }
    )

    // TODO 将统计的结果中超过阈值的用户信息拉人到黑名单中
    // java.io.NotSerializableException: com.alibaba.druid.pool.DruidPooledPreparedStatement
    // TODO 数据库连接对象是无法序列化的
    redultDS.foreachRDD(
      rdd => {
        val conn = JDBCUtil.getConnection()

        val pstat: PreparedStatement = conn.prepareStatement(
          """
            |insert into user_ad_count(dt, userid, adid, count)
            |values (?, ?, ?, ?)
            |on duplicate key
            |update count = count+?
            |""".stripMargin)

        val pstat1: PreparedStatement = conn.prepareStatement(
          """
            |insert into black_list(userid)
            |select userid from user_ad_count
            |where dt=? and userid=? and adid=? and count >= 100
            |on duplicate key
            |update userid = ?
            |""".stripMargin)


        rdd.foreach{
          case ((day, userid, adid), sum) => {
            pstat.setString(1, day)
            pstat.setString(2, userid)
            pstat.setString(3, adid)
            pstat.setLong(4, sum)
            pstat.setLong(5, sum)
            pstat.executeUpdate()

            pstat1.setString(1, day)
            pstat1.setString(2, userid)
            pstat1.setString(3, adid)
            pstat1.setString(4, userid)
            pstat1.executeUpdate()

            pstat.close()
            pstat1.close()
            conn.close()

          }

        }
      }
    )

  }

  /**
   * 数据分析
   *
   * @return
   */
  def analysis2() = {

    // TODO 读取kafka的数据
    val ds: DStream[String] = blackListDAO.readKafka()
//    ds.print()

    // TODO 将数据转换为样例类来使用
    val logDS: DStream[Ad_Click_Log] = ds.map(
      data => {
        val datas = data.split(" ")
        Ad_Click_Log(datas(0), datas(1), datas(2), datas(3), datas(4))
      }
    )

    // TODO 周期性地获取黑名单信息，判断当前用户的点击数据是否在黑名单中
    // Code(1)
    // Transform
    val redultDS: DStream[((String, String, String), Int)] = logDS.transform(
      rdd => {
        // Code: 周期性执行
        // TODO 读取MySQL数据库，获取黑名单信息
        val connection: Connection = JDBCUtil.getConnection()
        val pstat: PreparedStatement = connection.prepareStatement(
          """
            |select userid from black_list
            |""".stripMargin)
        val rs: ResultSet = pstat.executeQuery()
        // 黑名单的ID集合
        val blackIds = ListBuffer[String]()
        while (rs.next()) {
          blackIds.append(rs.getString(1))
        }
        rs.close()
        pstat.close()
        connection.close()

        // TODO 如果用户在黑名单中，那么将数据过滤掉，不会进行统计
        val filterRDD: RDD[Ad_Click_Log] = rdd.filter(
          log => {
            !blackIds.contains(log.userid)
          }
        )

        // TODO 将正常的数据进行点击量的统计
        // (key, 1) => (key, sum);  key => (day - uid - adid)
        // ts => day
        val sdf = new SimpleDateFormat("yyyy-MM-dd")
        val mapRDD: RDD[((String, String, String), Int)] = filterRDD.map(
          log => {
            val date = new Date(log.ts.toLong)
//            ((log.ts, log.userid, log.adid), 1)
            ((sdf.format(date), log.userid, log.adid), 1)
          }
        )
        mapRDD.reduceByKey(_ + _)

      }
    )

    // TODO 将统计的结果中超过阈值的用户信息拉人到黑名单中
    redultDS.foreachRDD(
      rdd => {
        rdd.foreach{
          case ((day, userid, adid), sum) => {
            // data 每一个采集周期中，用户点击同一个广告的数量
            // 有状态保存  updateStateByKey => checkpoint => HDFS => 产生大量小文件
            // 统计结果应该放在mysql(redis,过期数据)中  ps:Redis过期数据会自动删除

            // TODO 更新(新增)用户的点击数量
            val conn = JDBCUtil.getConnection()
            val pstat: PreparedStatement = conn.prepareStatement(
              """
                |insert into user_ad_count(dt, userid, adid, count)
                |values (?, ?, ?, ?)
                |on duplicate key
                |update count = count+?
                |""".stripMargin)

            pstat.setString(1, day)
            pstat.setString(2, userid)
            pstat.setString(3, adid)
            pstat.setLong(4, sum)
            pstat.setLong(5, sum)
            pstat.executeUpdate()

            // TODO 获取最新的统计数据
            // TODO 判断是否超过阈值
            // TODO 如果超过阈值，拉入到黑名单
            // insert into black_list select xxx from tableName where count > 100 update
            // insert ... select ...

            val pstat1: PreparedStatement = conn.prepareStatement(
              """
                |insert into black_list(userid)
                |select userid from user_ad_count
                |where dt=? and userid=? and adid=? and count >= 100
                |on duplicate key
                |update userid = ?
                |""".stripMargin)

            pstat1.setString(1, day)
            pstat1.setString(2, userid)
            pstat1.setString(3, adid)
            pstat1.setString(4, userid)
            pstat1.executeUpdate()

            pstat.close()
            pstat1.close()
            conn.close()

          }

        }
      }
    )

  }
}
