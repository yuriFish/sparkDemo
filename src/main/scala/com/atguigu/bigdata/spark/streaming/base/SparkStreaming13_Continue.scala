package com.atguigu.bigdata.spark.streaming.base

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming13_Continue {

  def main(args: Array[String]): Unit = {
    // TODO Spark环境
    val ssc = StreamingContext.getActiveOrCreate("./spark_streaming13_continue", getStreamingContext)

    ssc.start()
    // 等待采集器结束
    ssc.awaitTermination()

  }

  def getStreamingContext() : StreamingContext = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // SparkStreaming中的检查点不仅仅保存中间处理数据，还保存逻辑
    ssc.checkpoint("./spark_streaming13_continue")
    val ds: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    ds.print()

    ssc
  }
}
