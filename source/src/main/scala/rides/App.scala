package rides

import org.apache.spark.SparkContext._
import scala.io._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection._

object App {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Distributed ridesharing")
    val sc = new SparkContext(conf)

    val rides = sc.textFile("./input/cab_rides.csv")
    val weather = sc.textFile("./input/weather.csv")

    // extremely basic example of using weather data
    val weatherCount = weather.count()
    println(weatherCount)
  }
}
