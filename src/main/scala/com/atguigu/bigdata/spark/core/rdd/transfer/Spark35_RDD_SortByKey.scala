package com.atguigu.bigdata.spark.core.rdd.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark35_RDD_SortByKey {

  def main(args: Array[String]): Unit = {

    // TODO Spark - RDD - 算子(方法)
    //    sortByKey 在一个(K,V)的RDD上调用，K必须实现Ordered接口，返回一个按照key进行排序的

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val dataRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3), ("a", 4)), 1)

    // TODO 默认true升序
//    val sortRDD = dataRDD.sortByKey(true)
//    println(sortRDD.collect().mkString(","))

    // TODO    小功能：设置key为自定义类 User
    class User extends Ordered[User] with  Serializable {
      override def compare(that: User): Int = {
        1
      }
    }

    val userRDD: RDD[(User, Int)] = sc.makeRDD(
      List(
        (new User(), 1),
        (new User(), 2),
        (new User(), 3)
      )
    )

    val resultRDD = userRDD.sortByKey(true)
    println(resultRDD.collect().mkString(","))

    sc.stop()
  }

}
