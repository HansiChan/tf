package main

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}
import utils.RedisUtil

import scala.collection.mutable.ArrayBuffer

object Train {
  def main(args: Array[String]): Unit = {
    //将本次评估结果保存到下面这个文件中
    val writer = new PrintWriter(new File("model_training.txt"))

    //配置spark
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("TrafficTrain")
    val sc = new SparkContext(sparkConf)

    //redis的数据库索引
    val dbIndex = 1
    //获取redis中的实时数据
    val jedis = RedisUtil.pool.getResource
    //选择redis中的索引
    jedis.select(dbIndex)

    //想要对哪个监测点进行建模
    val monitorIDs = List("0005", "0015")
    //对上面两个监测点进行建模，可能需要一些目标监测点的相关监测点（比如某一段公路上的若干监测点）
    val monitorRelations = Map[String, Array[String]](
      "0005" -> Array("0003", "0004", "0005", "0006", "0007"),
      "0015" -> Array("0013", "0014", "0015", "0016", "0017"))

    //遍历上面所有的监测点
    val temp = monitorIDs.map(monitorID => {//本例中是：0005，0015
    //得到当前“目标监测点”的相关监测点（当然包含它自己），本例第一次循环拿出的是："0003", "0004", "0005", "0006", "0007"
    val monitorRelationList = monitorRelations.get(monitorID).get
      //时间处理
      //当前时间
      val currentDate = Calendar.getInstance().getTime
      //当前小时分钟数
      val hourMinuteSDF = new SimpleDateFormat("HHmm")
      //年月日格式化
      val dateSDF = new SimpleDateFormat("yyyyMMdd")
      //以当前时间格式化好的年月日时间
      val dateOfString = dateSDF.format(currentDate)

      //根据“相关监测点”取得当日的所有监测点当前的车辆速度与车辆数目的信息，例如：(0003,{1033=93_2, 1032=63_1, 1031=190_2, 1034=140_2, ...})
      val relationsInfo = monitorRelationList.map(monitorID => (monitorID, jedis.hgetAll(dateOfString + "_" + monitorID)))

      //使用多少小时内的数据进行建模，本例中使用x小时
      val hours = 1
      //本地特征向量
      val dataTrain = ArrayBuffer[LabeledPoint]()
      //特征因子数据集
      val dataX = ArrayBuffer[Double]()
      //特征因子对应的结果数据集
      val dataY = ArrayBuffer[Double]()
      //将时间拉回到2小时之前，单位为分钟，倒序
      for (i <- Range(60 * hours, 2, -1)) {
        dataX.clear()
        dataY.clear()
        //线性滤波
        for (index <- 0 to 3) {
          // 当前毫秒数 - 2小时之前毫秒数 + 2小时之前的后的0分钟，1分钟，2分钟，3分钟（第3分钟作为监督学习的结果向量）
          val oneMoment = currentDate.getTime - 60 * i * 1000 + i * index * 1000
          //当前小时与分钟
          val oneHM = hourMinuteSDF.format(new Date(oneMoment))
          //取出相关数据，v为Map集合，例如：(0005,{1033=93_2, 1032=63_1, 1031=190_2, 1034=140_2, ...})，长度为5
          for ((k, v) <- relationsInfo) {
            //如果已经得到2小时前的后2分钟，则说明下一时刻应该是结果向量，并且一定要确定是当前卡口
            if (k == monitorID && index == 3) {
              val nextMoment = oneMoment + 60 * 1000
              val nextHM = hourMinuteSDF.format(new Date(nextMoment))

              //判断是否有该时刻的数据，如果有，则读取之，并保存到结果特征因子dataY中，保存的是平均速度
              if (v.containsKey(nextHM)) {
                val speedAndCarCount = v.get(nextHM).split("-")
                val valueY = speedAndCarCount(0).toFloat / speedAndCarCount(1).toFloat
                dataY += valueY
              }
            }else if(index != 3){//去除掉第四时刻（即，假设的未来时刻）
              //判断是否有当前时刻的数据，如果有，则读取并保存到特征因子dataX中，如果缺失该部分数据，则为默认值-1.0
              if (v.containsKey(oneHM)) {
                val speedAndCarCount = v.get(oneHM).split("-")
                val valueX = speedAndCarCount(0).toFloat / speedAndCarCount(1).toFloat
                dataX += valueX
              } else {
                dataX += -1.0F
              }
            }
          }
        }
        //准备训练模型：
        if (dataY.toArray.length == 1) {
          val label = dataY.toArray.head
          //label范围为0~10，越大则道路越通畅，一个标签对应一组稠密特征因子（3~4个）
          val record = LabeledPoint(if (label.toInt / 10 < 10) label.toInt / 10 else 10, Vectors.dense(dataX.toArray))
          dataTrain += record
        }
      }
      //打印一下我们的特征数据集，至此，我们的特征数据集已经封装完毕
      dataTrain.foreach(println(_))

      //将特征数据集转为rdd数据集
      val rddData = sc.parallelize(dataTrain)
      //切分数据集，即所有已知的特征数据中，x%作为样本作为训练数据集，y%样本作为测试数据集
      val randomSplits = rddData.randomSplit(Array(0.6, 0.4), 11L)
      val trainingData = randomSplits(0)
      val testData = randomSplits(1)

      if(!rddData.isEmpty()){
        //使用熟练数据集训练model
        val model = new LogisticRegressionWithLBFGS().setNumClasses(11).run(trainingData)
        //使用测试数据集测试训练好的model
        val predictionAndLabels = testData.map {
          case LabeledPoint(label, features) =>
            val prediction = model.predict(features)
            (prediction, label)
        }
        //得到当前监测点model的评估值
        val metrics = new MulticlassMetrics(predictionAndLabels)
        val accuracy = metrics.accuracy
        println("评估值：" + accuracy)
        writer.write(accuracy.toString + "\r\n")
        //评估值范围为[0.0, 1.0]，越大model越优秀
        if (accuracy > 0.0) {
          //将模型保存到hdfs中，并将模型路径保存到redis中
          val hdfsPath = "hdfs://linux01:9000/traffic/model/" + monitorID + "_" + new SimpleDateFormat("yyyyMMddHHmmss").format(currentDate.getTime)
          model.save(sc, hdfsPath)
          jedis.hset("model", monitorID, hdfsPath)
        }
      }
    })
    //释放redis连接
    RedisUtil.pool.returnBrokenResource(jedis)
    writer.close()
  }
}
