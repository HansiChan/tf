package main

import java.text.DecimalFormat
import java.util
import java.util.Calendar

import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import utils.PropertyUtil

import scala.util.Random

object Producer {

  def main(args: Array[String]): Unit = {

    //数据读取与发送
    val props = PropertyUtil.properties
    val producer = new KafkaProducer[String,String](props)

    var startTime = Calendar.getInstance().getTimeInMillis/1000

    //数据模拟切换周期，每五分钟切换一次是否堵车
    val trafficCycle = 300
    while(true){
      val randomMonitorId = new DecimalFormat("0000").format(Random.nextInt(20) + 1)
      var randomSpeed = "000"

      val currentTime = Calendar.getInstance().getTimeInMillis/1000

      if(currentTime - startTime > trafficCycle){
        // 0 ~ 10
        randomSpeed = new DecimalFormat("000").format(Random.nextInt(10))
        if(currentTime - startTime > trafficCycle * 2){
          startTime = currentTime
        }
      }else{
        // 0 ~ 30 -> 30 ~ 60
        randomSpeed = new DecimalFormat("000").format(Random.nextInt(30) + 30 )
      }

      val jsonMap = new util.HashMap[String, String]()
      jsonMap.put("monitor_id", randomMonitorId)
      jsonMap.put("speed", randomSpeed)
      val event = JSON.toJSON(jsonMap)

      println("message sent: " + event)
      //数据发送
      producer.send(new ProducerRecord[String, String](PropertyUtil.getProperty("topic"), event.toString()))
      Thread.sleep(200)

    }
  }
}
