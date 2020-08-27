package com.atguigu.bigdata.spark.streaming.base

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object SparkStreaming03_Queue {

  def main(args: Array[String]): Unit = {
    // TODO Spark环境
    // SparkStreaming使用的核数最少是2  采集器至少一个，driver至少一个
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("streaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // TODO 执行逻辑
    // 从socket获取数据，一行一行获取
    val queue = new mutable.Queue[RDD[String]]()
    val queueDS: InputDStream[String] = ssc.queueStream(queue)

    queueDS.print()
    ssc.start()

    for (i <- 1 to 5) {
      val rdd: RDD[String] = ssc.sparkContext.makeRDD(List(i.toString))
      queue.enqueue(rdd)
      Thread.sleep(1000)  // 不知道有什么用，没有这一行也一样的输出
    }

    // 等待采集器的结束
    ssc.awaitTermination()

  }
}
