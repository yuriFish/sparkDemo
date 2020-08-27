package com.atguigu.bigdata.spark.core.acc_BC

import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark56_RDD_Acc04 {

  def main(args: Array[String]): Unit = {
    // TODO Spark acc 累加器: 分布式共享只写变量
    //   1. 将累加器变量注册到spark中
    //   2. 执行计算时，spark会将累加器发送到executor执行计算
    //   3. 计算完毕后，executor会将累加器的计算结果返回到driver端
    //   4. driver端获取到多个累计器的结果，然后两两合并，最后得到累加器的执行结果
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("acc")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List("hello spark", "hello", "spark", "scala"))
    // TODO 累加器: WordCount
    // TODO 自定义累加器

    //  1. 创建累加器
    val acc = new MyWordCountAccumulator
    //  2. 注册累加器
    sc.register(acc)

    //  3. 使用累加器
    rdd.flatMap(_.split(" ")).foreach{
      word => {
        acc.add(word)
      }
    }

    //  4. 获取累加器的值
    println("acc.value" + acc.value)


    sc.stop()
  }

  // TODO 自定义累加器
  //  1. 继承 LongAccumulatorV2，定义泛型 [IN, OUT]
  //                             IN: 累加器输入值的类型
  //                             OUT: 累加器返回结果的类型
  //  2. 重写方法(6个方法)
  //  3. copyAndReset must return a zero value copy
  class MyWordCountAccumulator extends AccumulatorV2 [String, mutable.Map[String, Int]]{
    // 存储WordCount的集合
    var wordCountMap = mutable.Map[String, Int]()

    // TODO 累加器是否初始化
    override def isZero: Boolean = {
      wordCountMap.isEmpty
    }

    // TODO 复制累加器
    override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = {
      new MyWordCountAccumulator
    }

    // TODO 重置累加器
    override def reset(): Unit = {
      wordCountMap.clear()
    }

    // TODO 向累加器中增加值
    override def add(word: String): Unit = {
//      wordCountMap(word) = wordCountMap.getOrElse(word, 0) + 1
      wordCountMap.update(word, wordCountMap.getOrElse(word, 0) + 1)
    }

    // TODO 合并当前累加器和其他累加器(累加器的合并)
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
      val map1 = wordCountMap
      val map2 = other.value

//      println("map1.keys=" + map1.keys)
//      println("map2.keys=" + map2.keys)
//      println("================")
      wordCountMap = map1.foldLeft(map2)(
        (map, kv) => {
//          println("kv._1=" + kv._1)
//          println("kv._2=" + kv._2)
          map(kv._1) = map.getOrElse(kv._1, 0) + kv._2
          map
        }
      )
    }

    // TODO 返回累加器的值(OUT)
    override def value: mutable.Map[String, Int] = {
      wordCountMap
    }
  }

}
