package com.atguigu.bigdata.spark.streaming.base

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming04_File {

  def main(args: Array[String]): Unit = {
    // TODO Spark环境
    // SparkStreaming使用的核数最少是2  采集器至少一个，driver至少一个
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("streaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // TODO 执行逻辑
    val dirDS: DStream[String] = ssc.textFileStream("input_streaming/")
    dirDS.print()


    ssc.start()
    // 等待采集器的结束
    ssc.awaitTermination()

  }
}
