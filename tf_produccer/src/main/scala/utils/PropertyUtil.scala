package utils

import java.util.Properties

object PropertyUtil {

  val properties = new Properties()

  //加载配置属性
  val inputStream = ClassLoader.getSystemResourceAsStream("kafka.properties")
  properties.load(inputStream)

  def getProperty(key:String):String = properties.getProperty(key)
}
