package main

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import utils.RedisUtil

import scala.collection.mutable.ArrayBuffer


object Prediction {
  def main(args: Array[String]): Unit = {
    //配置spark
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("TrafficTrain")
    val sc = new SparkContext(sparkConf)

    //想要预测的监测点
    val dateSDF = new SimpleDateFormat("yyyyMMdd")
    val hourMinuteSDF = new SimpleDateFormat("HHmm")
    val userSDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val inputDateString = "2017-12-19 21:19:20"
    val inputDate = userSDF.parse(inputDateString)

    val dayOfInputDate = dateSDF.format(inputDate)
    val hourMinuteOfInputDate = hourMinuteSDF.format(inputDate)

    val monitorIDs = List("0005", "0015")
    val monitorRelations = Map[String, Array[String]](
      "0005" -> Array("0003", "0004", "0005", "0006", "0007"),
      "0015" -> Array("0013", "0014", "0015", "0016", "0017"))

    //从redis中读取数据
    //redis的数据库索引
    val dbIndex = 1
    //获取redis中的实时数据
    val jedis = RedisUtil.pool.getResource
    jedis.select(dbIndex)
    val temp = monitorIDs.map(monitorID =>
    {
      val monitorRelationList = monitorRelations.get(monitorID).get
      val relationsInfo = monitorRelationList.map(monitorID => (monitorID, jedis.hgetAll(dayOfInputDate + "_" + monitorID)))
      //数据准备
      val dataX = ArrayBuffer[Double]()
      for(index <- Range(3, 0, -1)){
        val oneMoment = inputDate.getTime - 60 * index * 1000
        val oneHM = hourMinuteSDF.format(new Date(oneMoment))

        for((k, v) <- relationsInfo){
          if(v.containsKey(oneHM)){
            val speedAndCarCount = v.get(oneHM).split("-")
            val valueX = speedAndCarCount(0).toFloat / speedAndCarCount(1).toFloat
            dataX += valueX
          }else{
            dataX += -1.0F
          }
        }
      }
      println(Vectors.dense(dataX.toArray))
      //加载模型
      val modelPath = jedis.hget("model", monitorID)
      val model = LogisticRegressionModel.load(sc, modelPath)

      //预测
      val prediction = model.predict(Vectors.dense(dataX.toArray))
      println(monitorID + ",堵车评估值：" + prediction + "，是否通畅：" + (if(prediction >= 4) "通畅" else "不通畅"))

      //结果保存

      jedis.hset(inputDateString, monitorID, prediction.toString)
    })

    RedisUtil.pool.returnBrokenResource(jedis)
  }
}
