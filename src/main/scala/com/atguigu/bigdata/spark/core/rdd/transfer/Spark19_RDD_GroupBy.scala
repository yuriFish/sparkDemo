package com.atguigu.bigdata.spark.core.rdd.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark19_RDD_GroupBy {

  def main(args: Array[String]): Unit = {

    // TODO Spark - RDD - 算子(方法)
    //    groupBy 将数据根据指定的规则进行分组, 分区默认不变，但是数据会被打乱重新组合，我们将这样的操作称之为 shuffle
    //            极限情况下，数据可能被分在同一个分区中
    //            一个组的数据在一个分区中，但是并不是说一个分区中只有一个组

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 4, 2, 7, 5, 3, 6), 3)

    // TODO 分组
    // groupBy 将数据根据指定的规则进行分组，指定的规则的返回值就是分组的key
    // groupBy 返回值为元组，元组的第一个元素表示分组的key，第二个元素表示相同key的数据形成的可迭代集合
    // groupBy 执行完毕后，会将数据进行分组操作，但是分区不会改变。
    //          不同的组的数据会打乱在不同的分区

    //    val rdd: RDD[(Int, Iterable[Int])] = dataRDD.groupBy(
    //      num => {
    //        num % 4 // 2或4，然后注意 "output/groupBy/" 下的分区
    //      }
    //    )
    //    rdd.saveAsTextFile("output/groupBy/")
    //    println("分组后的数据分区的数量为: " + rdd.glom().collect().length)
    //    rdd.collect().foreach {
    //      case (key, list) => {
    //        println("key=" + key + ", list=[" + list.mkString(",") + "]")
    //      }
    //    }

    // TODO  小功能: 将List("Hello", "hive", "hbase", "Hadoop")根据单词首写字母进行分组。

    //    val func1_RDD1: RDD[String] = sc.makeRDD(List("Hello", "hive", "hbase", "Hadoop"))
    //    val func1_RDD2: RDD[(Char, Iterable[String])] = func1_RDD1.groupBy(
    //      data => {
    //        data.charAt(0)  // data(0) // String(0) 隐式转换
    //      }
    //    )
    //
    //    func1_RDD2.collect().foreach{
    //      case (key, list) => {
    //        println("key=" + key + ", list=[" + list.mkString(",") + "]")
    //      }
    //    }

    // TODO  小功能: 从服务器日志数据apache.log中获取每个时间段访问量 (我这里是每个小时)

    val func2_RDD1: RDD[String] = sc.textFile("input/apache.log")
    val func2_RDD2: RDD[String] = func2_RDD1.map(
      lines => {
        lines.split(" ")(3).substring(0, 13)
      }
    )

    val func2_RDD3: RDD[(String, Iterable[String])] = func2_RDD2.groupBy(data => data)

//     1. 直接输出
    func2_RDD3.collect().foreach{
      case (key, list) => {
        println("key=" + key + ", length=" + list)
      }
    }

//     2. 将list的个数统计出来，再输出
        val func2_RDD4: RDD[(String, Int)] = func2_RDD3.map(
          lines => {
            (lines._1, lines._2.size)
          }
        )
        func2_RDD4.collect().foreach(println)

    // 上面 1和2 的结合
//    val func2_RDD3: RDD[(String, Int)] = func2_RDD2.groupBy(lines => lines).map(
//      data => {
//        (data._1, data._2.size)
//      }
//    )
//    func2_RDD3.collect().foreach(println)

    // TODO  小功能: WordCount
//    val wordRDD: RDD[String] = sc.makeRDD(List("Hello Spark", "Hello Scala!"))
//    println(wordRDD
//      .flatMap(_.split(" "))
//      .groupBy(word => word)
//      .map(kv => (kv._1, kv._2.size))
//      .collect().mkString(","))


    sc.stop()
  }

}
