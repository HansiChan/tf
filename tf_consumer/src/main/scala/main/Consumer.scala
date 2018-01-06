package main

import java.text.SimpleDateFormat
import java.util.Calendar

import com.alibaba.fastjson.{JSON, TypeReference}
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import utils.{PropertyUtil, RedisUtil}

object Consumer {

  def main(args: Array[String]): Unit = {
    //初始化Spark
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("TrafficStreaming")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc,Seconds(5))
    ssc.checkpoint("./ssc/checkpoint")

    //配置Kafka参数
    val kafkaParams = Map("metadata.broker.list"->PropertyUtil.getProperty("metadata.broker.list")
      , "serializer.class" -> PropertyUtil.getProperty("serializer.class"))

    //配置Kafka主题
    val topics = Set(PropertyUtil.getProperty("topic"))

    //读取Kafka主题中的每一条数据
    val kafkaLineDStream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topics).map(_._2)
    val event = kafkaLineDStream.map(line=>{
      //使用fastJSON解析基于事件的每一条数据到Java的HashMap中
      val lineJavaMap = JSON.parseObject(line, new TypeReference[java.util.Map[String,String]]() {})
      //将Java的HashMap转换为Scala的Mutable.Map
      import scala.collection.JavaConverters._
      val lineScalaMap:collection.mutable.Map[String,String] = mapAsScalaMapConverter(lineJavaMap).asScala
      println(lineScalaMap)
      lineScalaMap
    })


    //将每一条数据按照monitor_id聚合，聚合时每一条数据中的“车辆速度”叠加，“车辆个数”叠加
    //(monitor_id, (speed, 1))例如：(0005, (80,1))
    val sumOfSpeedAndCount = event.map(e => (e.get("monitor_id").get, e.get("speed").get)).mapValues(s => (s.toInt, 1))
      //timeWindow为20秒，slideTime为10秒，严格意义应该设置为：60, 60,即，每隔60秒，统计60秒内的数据，因为我们打算按照分钟来采集数据
      .reduceByKeyAndWindow((t1: (Int, Int), t2: (Int, Int)) => (t1._1 + t2._1, t1._2 + t2._2), Seconds(20), Seconds(10))

    //数据库索引号
    val dbIndex = 1
    //将采集到的数据，按照每分钟采集到redis中，用于后边的建模与分析
    sumOfSpeedAndCount.foreachRDD(rdd=>{
      rdd.foreachPartition(partitionRecords=>{
        //tuple:(monitor_id,(speed,1))
        partitionRecords.filter((tuple:(String,(Int,Int)))=>tuple._2._2 > 0).foreach(pair=>{
          val jedis = RedisUtil.pool.getResource
          val monitorId = pair._1
          val sumOfSpeed = pair._2._1
          val sumOfCarCount = pair._2._2

          //模拟数据为实时流入，则获取当前分钟作为车辆经过的分钟
          val currentTime = Calendar.getInstance().getTime
          val hourMinuteSDF = new SimpleDateFormat("HHmm")
          val dataSDF = new SimpleDateFormat("yyyyMMdd")

          val hourMinuteTime = hourMinuteSDF.format(currentTime)
          val data = dataSDF.format(currentTime)

          //选择存入的数据库
          jedis.select(dbIndex)
          jedis.hset(data + "_" + monitorId,hourMinuteTime,sumOfSpeed + "-" + sumOfCarCount)
          println(data + "-" + monitorId)
          RedisUtil.pool.returnResource(jedis)
        })
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
