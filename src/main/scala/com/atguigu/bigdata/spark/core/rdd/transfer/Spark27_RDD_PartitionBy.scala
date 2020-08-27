package com.atguigu.bigdata.spark.core.rdd.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.HashPartitioner

object Spark27_RDD_PartitionBy {

  def main(args: Array[String]): Unit = {

    // TODO Spark - RDD - 算子(方法)
    //    	Key - Value类型
    //    PartitionBy 将数据按照指定Partitioner重新进行分区。Spark默认的分区器是HashPartitioner
    //                分区器对象 HashPartitioner & RangePartitioner
    //                sortBy使用了RangePartitioner

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO Spark很多方法都是基于key进行操作的，所以数据格式应该为键值对(对偶元组)
    // 隐式转换
    val dataRDD: RDD[(Int, String)] = sc.makeRDD(Array((1,"aaa"),(2,"bbb"),(3,"ccc")),3)

    // TODO partitionBy: 根据指定的规则对数据进行分区
    //      groupBy
    //      filter => coalesce
    //      repartition => shuffle
    val rdd1: RDD[(Int, String)] = dataRDD.partitionBy(new HashPartitioner(2))

    println(rdd1.collect().mkString(","))

    // TODO 思考一个问题：如果重分区的分区器和当前RDD的分区器一样怎么办？(分区数量也一样)
    //  不进行任何处理。不会再进行重分区
    val rdd2: RDD[(Int, String)] = dataRDD.partitionBy(new HashPartitioner(3))
    val rdd3: RDD[(Int, String)] = rdd2.partitionBy(new HashPartitioner(3))

    // TODO 思考一个问题：Spark还有其他分区器吗？
    //  HashPartitioner & RangePartitioner

    // TODO 思考一个问题：如果想按照自己的方法进行数据分区怎么办？
    //  自定义分区器  eg: Spark28_RDD_MyPartitioner

    // TODO 思考一个问题：哪那么多问题？


    sc.stop()
  }

}
