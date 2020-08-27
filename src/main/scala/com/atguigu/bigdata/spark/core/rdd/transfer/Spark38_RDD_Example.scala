package com.atguigu.bigdata.spark.core.rdd.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark38_RDD_Example {

  def main(args: Array[String]): Unit = {

    // TODO 1)	数据准备
    //          agent.log：时间戳，省份，城市，用户，广告，中间字段使用空格分隔。
    //      2)	需求描述
    //          统计出每一个省份每个广告被点击数量排行的Top3
    //      3)	需求分析
    //      4)	功能实现

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Example")
    val sc = new SparkContext(sparkConf)

    //  TODO 1. 获取原始数据
    val dataRDD: RDD[String] = sc.textFile("input/agent.log")

    //  TODO 2. 将原始数据进行结构的转换，方便统计 => ((省份, 广告), 1) / ("省份-广告", 1)
    val mapRDD1: RDD[(String, Int)] = dataRDD.map(
      lines => {
        val datas = lines.split(" ")
        (datas(1) + "-" + datas(4), 1)
      }
    )

    //  TODO 3. 将相同key的数据进行分组聚合
    val reduceRDD: RDD[(String, Int)] = mapRDD1.reduceByKey(_ + _)

    //  TODO 4. 将聚合后的结果进行结构转换 => (省份, (广告, sum))
    val mapRDD2: RDD[(String, (String, Int))] = reduceRDD.map {
      case (key, sum) => {
        val keys = key.split("-")
        (keys(0), (keys(1), sum))
      }
    }

    //  TODO 5. 将相同的省份的数据放在一个组中 => (省份, Iterator[(广告1, sum), (广告2, sum)])
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD2.groupByKey()

    //  TODO 6. 将分组后的数据进行排序(降序), 取前3, TOP3
    //  mapValues
    val resultRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
      iter => {
        iter.toList.sortWith(
          (left, right) => {
            left._2 > right._2
          }
        ).take(5)
      }
    )

    //  TODO 7. 将数据采集到控制台进行打印
    resultRDD.collect().foreach(println)


    sc.stop()
  }

}
