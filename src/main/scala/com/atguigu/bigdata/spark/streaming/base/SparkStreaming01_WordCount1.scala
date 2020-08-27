package com.atguigu.bigdata.spark.streaming.base

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming01_WordCount1 {

  def main(args: Array[String]): Unit = {
    // TODO Spark环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // TODO 执行逻辑
    // 从socket获取数据，一行一行获取
    val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    val wordDS: DStream[String] = socketDS.flatMap(_.split(" "))

    val wordToOneDS: DStream[(String, Int)] = wordDS.map((_, 1))

    val wordToSumDS: DStream[(String, Int)] = wordToOneDS.reduceByKey(_ + _)

    wordToSumDS.print()

    // TODO 关闭连接
    // Driver程序执行Streaming处理过程中不能结束
    // 采集器在正常情况下启动后不应该停止，除非特殊情况
//    ssc.stop()
    ssc.start()
    // 等待采集器的结束
    ssc.awaitTermination()

  }
}
