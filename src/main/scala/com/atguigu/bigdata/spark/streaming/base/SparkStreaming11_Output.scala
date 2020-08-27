package com.atguigu.bigdata.spark.streaming.base

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming11_Output {

  def main(args: Array[String]): Unit = {
    // TODO Spark环境
    // 自定义数据采集器
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    ssc.sparkContext.setCheckpointDir("checkpoint/spark_streaming08/")

    val ds: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    // TODO 窗口
    val wordToOneDS: DStream[(String, Int)] = ds.map(
      num => ("key", num.toInt)
    )

    // TODO reduceByKeyAndWindow方法一般用于重复数据的范围比较大的场合，这样可以优化效率
    // window(windowLength, slideInterval)
    // reduceByKeyAndWindow(func, invFunc, windowLength, slideInterval, [numTasks])
    val resultDS: DStream[(String, Int)] = wordToOneDS.reduceByKeyAndWindow(
      (x, y) => {
        println(s"x=${x}, y=${y}")
        x + y
      },
      (a, b) => {
        println(s"a=${a}, b=${b}")
        a - b
      },
      Seconds(9)
    )

//    resultDS.print()
//    resultDS.saveAsTextFiles("output/spark_streaming11")

    // transform : rdd => rdd 有返回
    // foreachRDD : rdd => Unit 无返回
    resultDS.foreachRDD(rdd => {
      rdd.foreach(
        data => {
          println(data)
        }
      )
    })

    // 开启任务
    ssc.start()
    ssc.awaitTermination()
  }
}
