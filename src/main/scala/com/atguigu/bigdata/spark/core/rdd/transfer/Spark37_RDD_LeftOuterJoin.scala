package com.atguigu.bigdata.spark.core.rdd.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark37_RDD_LeftOuterJoin {

  def main(args: Array[String]): Unit = {

    // TODO Spark - RDD - 算子(方法)
    //    leftOuterJoin 类似于SQL语句的左外连接

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val dataRDD1: RDD[(Int, String)] = sc.makeRDD(Array((1, "a"), (1, "e"), (2, "b"), (3, "c"), (5, "d")))
    val dataRDD2: RDD[(Int, Int)] = sc.makeRDD(Array((1, 4), (1, 9), (2, 5), (3, 6), (4, 1)))

    // 还有 rightOuterJoin
//    val resultRDD1 = dataRDD1.leftOuterJoin(dataRDD2)
//    resultRDD1.collect().foreach(println)

    // TODO cogroup 在类型为(K,V)和(K,W)的RDD上调用，返回一个(K,(Iterable<V>,Iterable<W>))类型的RDD
    val resultRDD2: RDD[(Int, (Iterable[String], Iterable[Int]))] = dataRDD1.cogroup(dataRDD2)
    resultRDD2.collect().foreach(println)

    sc.stop()
  }

}
