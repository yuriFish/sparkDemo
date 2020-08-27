package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, MapType, StringType, StructField, StructType}

object SparkSQL13_exam3 {

  def main(args: Array[String]): Unit = {

    // TODO 创建环境对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    // builder 构建，创建
    // TODO 默认情况下SparkSQL支持本地(内置)Hive操作，执行前需要启用hive支持 .enableHiveSupport()
    // TODO 访问外置的Hive时，需要将(虚拟机上)配置好的hive-site.xml文件放在 src/main/resources 和 target/classes文件夹下 并且hive要启动
    val spark = SparkSession.builder()
      .enableHiveSupport()  // 启用hive支持  会在根目录创建 metastore_db和spark-warehouse文件夹
      .config(sparkConf).getOrCreate()
    // 导入隐式转换，这里的spark其实是环境对象的名称
    // 要求这个对象使用val声明
    import spark.implicits._

    // TODO SparkSQL
//    spark.sql("use atguigu200720")  // 外置hive的database，load数据时，table需要 eg：atguigu200720.user_visit_action

    // TODO 从Hive表中获取满足条件的数据
    spark.sql(
      """
        |select
        |    a.*,
        |    c.area,
        |    p.product_name,
        |    c.city_name
        |from user_visit_action a
        |join city_info c on c.city_id = a.city_id
        |join product_info p on p.product_id = a.click_product_id
        |where a.click_product_id > -1
        |""".stripMargin).createOrReplaceTempView("t1")

    // TODO 将数据根据区域和商品进行分组，统计商品点击的数量
    // 北京，上海，北京，深圳
    // ******************
    // in: cityname:String
    // buffer: (total, map)
    // out: remark:String
    // (商品点击总和，每个城市的点击总和)
    // (商品点击总和，Map(城市，点击sum))
    // 城市点击sum / 商品点击总和 *100%

    // TODO 创建自定义聚合函数
    val udaf = new CityRemarkUDAF
    spark.udf.register("cityRemark", udaf)

    spark.sql(
      """
        |select
        |    area,
        |    product_name,
        |    count(*) as clickCount,
        |    cityRemark(city_name)
        |from t1 group by area, product_name
        |""".stripMargin).createOrReplaceTempView("t2")

    // TODO 将统计的数量根据数量进行排序(降序)
    spark.sql(
      """
        |select
        |    *,
        |    rank() over(partition by area order by clickCount desc) as rank
        |from t2
        |""".stripMargin).createOrReplaceTempView("t3")

    // TODO 将组内排序后的结果取前3名
    spark.sql(
      """
        |select
        |    *
        |from t3
        |where rank <= 3
        |""".stripMargin).show()

    // TODO 释放对象
    spark.stop()
  }

  // 自定义聚合函数
  class CityRemarkUDAF extends UserDefinedAggregateFunction {
    // TODO 输入的数据其实就是城市的名称
    override def inputSchema: StructType = {
      StructType(Array(StructField("cityName", StringType)))
    }

    // TODO 缓冲区的数据应该为: totalCnt, Map[cityName, cnt]
    override def bufferSchema: StructType = {
      StructType(Array(
        StructField("totalCnt", LongType),
        StructField("cityMap", MapType(StringType, LongType)),
      ))
    }

    // TODO 返回城市备注的字符串
    override def dataType: DataType = StringType

    // TODO 稳定性
    override def deterministic: Boolean = true

    // TODO 缓冲区的初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L
//      buffer.update(0, 0L)
      buffer(1) = Map[String, Long]()
    }

    // TODO 更新缓冲区
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      val cityName = input.getString(0)
      // 点击总和需要增加
      buffer(0) = buffer.getLong(0) + 1
      // 城市点击增加
      val cityMap: Map[String, Long] = buffer.getAs[Map[String, Long]](1)

      val cityClickCount = cityMap.getOrElse(cityName, 0L) + 1

      buffer(1) = cityMap.updated(cityName, cityClickCount)
    }

    // TODO 合并缓冲区
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      // 合并点击数量总和
      buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
      // 合并城市点击Map
      val map1: Map[String, Long] = buffer1.getAs[Map[String, Long]](1)
      val map2: Map[String, Long] = buffer2.getAs[Map[String, Long]](1)

//      println("map2.keys -> " + map2.keys + ", map2.values -> " + map2.values)

      buffer1(1) = map1.foldLeft(map2) {
        case (map, (k, v)) => {
          map.updated(k, map.getOrElse(k, 0L) + v)
        }
      }

      println("map1.keys -> " + map1.keys + ", map1.values -> " + map1.values + ", map2.keys -> " + map2.keys + ", map2.values -> " + map2.values)

    }

    // 48 51 43 56 54 45 51 48

    // TODO 对缓冲区进行计算并返回备注信息
    override def evaluate(buffer: Row): Any = {
      val totalCnt: Long = buffer.getLong(0)
      val cityMap: collection.Map[String, Long] = buffer.getMap[String, Long](1)

      val cityToCountList: List[(String, Long)] = cityMap.toList.sortWith(
        (left, right) => left._2 > right._2
      ).take(2)

      val hasRest = cityMap.size > 2
      var rest = 0L

      val sb = new StringBuilder
      cityToCountList.foreach{
        case (city, cnt) => {
          val r = cnt * 100 / totalCnt
          sb.append(city + " " + r + "%, ")
          rest = rest + r
        }
      }

      if (hasRest) {
        sb.append("其他: " + (100-rest) + "%" )
      }

      sb.toString()
    }
  }
}
