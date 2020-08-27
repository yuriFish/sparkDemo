package com.atguigu.bigdata.spark.core.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark44_RDD_Save {

  def main(args: Array[String]): Unit = {

    // TODO Spark - RDD - 算子 - 行动
    //    save 相关算子

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Action")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))

    // TODO save
    // 保存成Text文件
    rdd.saveAsTextFile("output/save_asTextFile")

    // 序列化成对象保存到文件
    rdd.saveAsObjectFile("output/save_asObjectFile")

    // 保存成Sequencefile文件
    rdd.map((_,1)).saveAsSequenceFile("output/save_asSequenceFile")



    sc.stop()
  }

}
