package com.atguigu.bigdata.spark.streaming.base

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming09_Window {

  def main(args: Array[String]): Unit = {
    // TODO Spark环境
    // 自定义数据采集器
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    ssc.sparkContext.setCheckpointDir("checkpoint/spark_streaming08/")

    val ds: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    // TODO 窗口
//    val ints: Array[Int] = Array(1, 2, 3, 4, 5, 6, 7, 8)
//    ints.sliding(3)

    val wordDS: DStream[String] = ds.flatMap(_.split(" "))
    val wordToOneDS: DStream[(String, Int)] = wordDS.map((_, 1))

    // TODO 将多个采集周期作为计算的整体
    // 窗口的范围应该是采集周期的整数倍
    // 默认滑动的幅度(步长)为一个采集周期
//    val windowDS: DStream[(String, Int)] = wordToOneDS.window(Seconds(9))
    // 窗口的计算周期等于窗口的滑动步长
    // 窗口的范围大小和滑动步长应该都是采集周期的整数倍
    val windowDS: DStream[(String, Int)] = wordToOneDS.window(Seconds(9), Seconds(6))
    val resultDS: DStream[(String, Int)] = windowDS.reduceByKey(_ + _)

    resultDS.print()

    // 开启任务
    ssc.start()
    ssc.awaitTermination()
  }
}
