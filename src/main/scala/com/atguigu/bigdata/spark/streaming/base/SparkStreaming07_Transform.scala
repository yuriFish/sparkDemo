package com.atguigu.bigdata.spark.streaming.base

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming07_Transform {

  def main(args: Array[String]): Unit = {
    // TODO Spark环境
    // 自定义数据采集器
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // TODO
    val ds: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    // TODO 转换
    // Code Driver端执行 (执行1次)
//    val newDS: DStream[String] = ds.transform(
//      rdd => {
//        // Code Driver端执行 (执行M次)
//        rdd.map(
//          data => {
//            // Code Executor端执行 (执行N次)
//            data * 2
//          }
//        )
//      }
//    )
//    // Code Driver端执行 (执行1次)
//    val newDS1: DStream[String] = ds.map(
//      data => {
//        // Code Executor端执行 (执行N次)
//        data * 2
//      }
//    )
//    newDS1.print()

    //转换为RDD操作
    val wordAndCountDStream: DStream[(String, Int)] = ds.transform(rdd => {

      val words: RDD[String] = rdd.flatMap(_.split(" "))

      val wordAndOne: RDD[(String, Int)] = words.map((_, 1))

      val value: RDD[(String, Int)] = wordAndOne.reduceByKey(_ + _)

      value
    })

    //打印
    wordAndCountDStream.print()


    // 开启任务
    ssc.start()
    ssc.awaitTermination()


  }
}
