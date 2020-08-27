package com.atguigu.bigdata.spark.core.rdd.serial

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark46_RDD_Serializable01 {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Action")
    val sc = new SparkContext(sparkConf)

    //  TODO Spark 序列化
//    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
//    rdd.foreach(
//      num => {
//        val user = new User()
//        println("age = " + (user.age + num))
//      }
//    )

    //  TODO Exception: Task not serializable
    //    NotSerializableException: com.atguigu.bigdata.spark.core.rdd.serial.Spark46_RDD_Serializable$User
    //    解决方法1：class User {...} =>  class User extends Serializable {...}
    //    解决方法2: class User {...} =>  case class User(age: Int = 20)
    //  val user = new User() 在Driver端，rdd.foreach 在Executor端
//    val user = new User()
//    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
//    rdd.foreach(
//      num => {
//        println("age = " + (user.age + num))
//      }
//    )

    // TODO Scala 闭包
    val user = new User()
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3))
    rdd.foreach(
      num => {
        println("age = " + (user.age + num))
      }
    )

    sc.stop()
  }

  class User extends Serializable {
    val age: Int = 20
  }

  // 样例类自动混入可序列化特质
//  case class User(age: Int = 20)

//    class User {
//      val age: Int = 20
//    }
}
