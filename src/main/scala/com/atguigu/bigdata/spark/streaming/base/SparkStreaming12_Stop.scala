package com.atguigu.bigdata.spark.streaming.base

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

object SparkStreaming12_Stop {

  def main(args: Array[String]): Unit = {
    // TODO Spark环境
    // 自定义数据采集器
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val ds: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    // TODO 窗口
    val wordToOneDS: DStream[(String, String)] = ds.map(
      num => ("key", num)
    )

    wordToOneDS.print()

    // 开启任务
    ssc.start()

    // TODO 当业务升级的场合，或逻辑发生变化
    // TODO Stop方法一般不会放置在main线程完成
    // TODO 需要将stop方法使用新的线程完成调用
    //    ssc.stop()
    new Thread(new Runnable {
      override def run(): Unit = {
        // TODO stop方法不应该在线程启动后马上调用
        // TODO stop方法调用的时机，这个时机不容易确定，需要周期性地判断时机是否出现
        while (true) {
          Thread.sleep(15000)
          // TODO 关闭时机的判断一般不会使用业务操作
          // TODO 一般采用第三方的程序或存储进行判断
          // HDFS => /stopSpark
          // zk，mysql，redis
          // 优雅地关闭
//          ssc.stop(true, true)
          val state: StreamingContextState = ssc.getState()
          if (state == StreamingContextState.ACTIVE) {
            ssc.stop(true, true)
          }
          // TODO SparkStreaming如果停止后，当前的线程也应该同时停止
          System.exit(0)
        }
      }
    }).start()

    // 等待采集器结束
    ssc.awaitTermination()



    // 线程
//    val t = new Thread()
//    t.start()
//    t.stop()  // 线程安全, 准确来说是数据安全, stop不推荐使用
  }
}
