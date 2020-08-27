package com.atguigu.bigdata.spark.core.rdd.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark30_RDD_GroupByKey {

  def main(args: Array[String]): Unit = {

    // TODO Spark - RDD - 算子(方法)
    //    	Key - Value类型
    //    GroupByKey 将分区的数据直接转换为相同类型的内存数组进行后续处理

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO Spark很多方法都是基于key进行操作的，所以数据格式应该为键值对(对偶元组)
    val dataRDD: RDD[(String, Int)] = sc.makeRDD(List(("hello", 1), ("spark", 1), ("hello", 1), ("scala", 1)))

    // TODO
    val rdd1: RDD[(String, Iterable[Int])] = dataRDD.groupByKey()
    val rdd2 = dataRDD.groupByKey(2)
    val rdd3 = dataRDD.groupByKey(new HashPartitioner(2))

    val word2count: RDD[(String, Int)] = rdd1.map {
      case (index, iter) => {
        (index, iter.sum)
      }
    }

    println(word2count.collect().mkString(","))

    println("rdd1 ----------")
    rdd1.collect().foreach(println)
    println("rdd2 ----------")
    rdd2.collect().foreach(println)
    println("rdd3 ----------")
    rdd3.collect().foreach(println)

    // TODO 思考一个问题: reduceByKey和groupByKey的区别？
    //  两个算子在实现相同的业务功能时，reduceByKey存在预聚合功能，所以性能比较高，推荐使用
    //  但是，不是说一定就采用这个方法，需要根据场景来选择

    sc.stop()
  }

}
