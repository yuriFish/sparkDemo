package com.atguigu.bigdata.spark.streaming.base

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming08_State {

  def main(args: Array[String]): Unit = {
    // TODO Spark环境
    // 自定义数据采集器
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    ssc.sparkContext.setCheckpointDir("checkpoint/spark_streaming08/")

    // TODO
    val ds: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    // TODO 数据的有状态的保存
    // 将Spark每个采集周期数据的处理结果保存起来，然后和后续的数据进行聚合

    // 一次性，reduceByKey是无状态数据操作，而保存结果需要有状态的数据操作
//    ds.flatMap(_.split(" "))
//        .map((_, 1))
//        .reduceByKey(_+_)
//        .print()

    // 有状态的目的其实就是将每一个采集周期数据的计算结果临时保存起来
    // 然后再一次数据的处理中可以继续使用
    ds.flatMap(_.split(" "))
      .map((_, 1L))
      // updateStateByKey 是有状态计算方法
      //   第一个参数表示，相同的key的value集合
      //   第二个参数表示，想用key的缓冲区的数据，有可能为空
      // 这里的计算的中间结果，需要保存到检查点的位置中，所以需要设定检查点路径
      .updateStateByKey[Long](
        // Option: Sum, None
        (seq:Seq[Long], buffer:Option[Long]) => {
          val newBufferValue: Long = buffer.getOrElse(0L) + seq.sum
          Option(newBufferValue)
        }
      )
      .print()

    // 开启任务
    ssc.start()
    ssc.awaitTermination()
  }
}
