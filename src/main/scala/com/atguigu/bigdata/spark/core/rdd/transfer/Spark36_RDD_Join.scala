package com.atguigu.bigdata.spark.core.rdd.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark36_RDD_Join {

  def main(args: Array[String]): Unit = {

    // TODO Spark - RDD - 算子(方法)
    //    join 在类型为(K,V)和(K,W)的RDD上调用，返回一个相同key对应的所有元素连接在一起的(K,(V,W))的RDD

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val dataRDD1: RDD[(Int, String)] = sc.makeRDD(Array((1, "a"), (1, "e"), (2, "b"), (3, "c"), (5, "d")))
    val dataRDD2: RDD[(Int, Int)] = sc.makeRDD(Array((1, 4), (1, 9), (2, 5), (3, 6), (4, 1)))

    // join 性能不太高，能不用就不用
    val resultRDD: RDD[(Int, (String, Int))] = dataRDD1.join(dataRDD2)
    resultRDD.collect().foreach(println)

    // TODO 思考一个问题：如果key存在不相等呢？
    //   对应的数据无法连接。 如果key存在重复，则会重复连接。

    sc.stop()
  }

}
