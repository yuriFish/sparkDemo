package com.atguigu.bigdata.spark.core.rdd.basic

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_File_Par {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("wordCount")
    val sc = new SparkContext(sparkConf)

    // TODO Spark - 从磁盘(File)中创建RDD
    // textFile 第一个参数: 读取文件的路径
    // textFile 第二个参数(minPartitions = Int): 最小分区的数量
    //          默认值: math.min(defaultParallelism, 2)
//    val fileRDD1: RDD[String] = sc.textFile("input/file_par.txt", minPartitions = 2)
//    fileRDD1.saveAsTextFile("output/file_par_1")

//    val fileRDD2: RDD[String] = sc.textFile("input/file_par.txt", minPartitions = 3)
//    fileRDD2.saveAsTextFile("output/file_par_2")
//
//    val fileRDD3: RDD[String] = sc.textFile("input/file_par.txt", minPartitions = 4)
//    fileRDD3.saveAsTextFile("output/file_par_3")

    // textFile 读取文件时，默认采用Hadoop读取文件的规则
    //    文件切片规则: 以字节方式来切片
    //    数据读取规则: 以行为方式来读取

    // TODO 文件到底切成几片? (分区的数量) 以下面代码为例，文件以 file_par.txt为例
    // 文件字节数(10)，切片数 minPartitions = 2  (每个回车和换行各占一个字节, 数字1-4，3个回车和3个换行，共10个字节)
    // 10 / 2 = 5
    // 若字节数为 11， 11 / 2 = 5...1 => 3(个分区)
    // 所谓的最小分区数，取决于总的字节数是否能整除分区数并且剩余的字节达到一个比率
    // 实际产生的分区可能大于最小分区数

    // TODO 分区的数据如何存储?
    // 分区数据是以行为单位读取的，而不是字节
    // file_par.txt 如果数据只有一行，则只有第一个分区有数据，第二个分区为空
    val fileRDD4: RDD[String] = sc.textFile("input/file_par.txt", minPartitions = 2)
    fileRDD4.saveAsTextFile("output/file_par_4")

    // TODO 分几个区?
    //  10 / 4 = 2...2 => 5(个分区)
    //  0 => (0, 2)
    //  1 => (2, 4)
    //  2 => (4, 6)
    //  3 => (6, 8)
    //  4 => (8, 10)
    // TODO 如何存储?
    //  数据是以行为单位读取的，但是会考虑偏移量(offset)的设置
    //  1@@ => 012
    //  2@@ => 345
    //  3@@ => 678
    //  4@@ => 9

    //  0 => (0, 2) => 1
    //  1 => (2, 4) => 2
    //  2 => (4, 6) => 3
    //  3 => (6, 8) =>
    //  4 => (8, 10) => 4
    val fileRDD5: RDD[String] = sc.textFile("input/file_par.txt", minPartitions = 4)
    fileRDD5.saveAsTextFile("output/file_par_5")

    // TODO 多文件时，hadoop是以文件为单位进行划分的，读取数据不能跨文件
    val fileRDD6: RDD[String] = sc.textFile("input/word*.txt", minPartitions = 3)
    fileRDD6.saveAsTextFile("output/file_par_6")

    sc.stop()
  }

}
