package com.atguigu.bigdata.spark.core.rdd.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark34_RDD_Pipe {

  def main(args: Array[String]): Unit = {

    // TODO Spark - RDD - 算子(方法)
    //    	Key - Value类型
    //    Pipe

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    sc.stop()
  }

}
