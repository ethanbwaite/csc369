package rides

import org.apache.spark.SparkContext._
import scala.io._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection._

import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.Instant

case class Ride(
  val distance: Float,
  val cabType: String,
  val day: String,
  val hour: Int,
  val destination: String,
  val source: String,
  val price: Double,
  val surgeMultiplier: Double,
  val name: String
);

case class Weather(
  val temp: Float,
  val location: String,
  val clouds: Float,
  val pressure: Float,
  val rain: Float,
  val day: String,
  val hour: Int,
  val humidity: Float,
  val wind: Float
);

object App {
  def doubleOrDefault(f: String, default: Double): Double = {
    if (f == "") return default
    return f.toDouble
  }

  def floatOrZero(f: String): Float = {
    val default: Double = 0.0
    if (f == "") return default.toFloat
    return f.toFloat
  }

  def timestampToDayHour(timestamp: Long): (String, Int) = {
    // ZonedDateTime object in Boston time (UTC-5)
    // https://docs.oracle.com/javase/8/docs/api/java/time/ZonedDateTime.html
    val dt = ZonedDateTime.ofInstant(Instant.ofEpochSecond(timestamp), ZoneId.of("UTC-5"))
    return (dt.getDayOfWeek.toString, dt.getHour)
  }

  def parseRide(in: String): Ride = {
    val s = in.split(",")

    val distance = floatOrZero(s(0))
    val cabType = s(1)
    val time = timestampToDayHour(s(2).toLong)
    val destination = s(3)
    val source = s(4)
    val price = doubleOrDefault(s(5), -1.0)
    val surge = doubleOrDefault(s(6), 1.0)
    val name = s(9)

    val ride = Ride(distance, cabType, time._1, time._2, destination, source, price, surge, name)
    return ride
  }

  def parseWeather(in: String): Weather = {
    val s = in.split(",")

    val temp = floatOrZero(s(0))
    val location = s(1)
    val clouds = floatOrZero(s(2))
    val pressure = floatOrZero(s(3))
    val rain = floatOrZero(s(4))
    val time = timestampToDayHour(s(5).toLong)
    val humidity = floatOrZero(s(6))
    val wind = floatOrZero(s(7))

    val weather = Weather(temp, location, clouds, pressure, rain, time._1, time._2, humidity, wind)
    return weather
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val LOCAL = false    // change some config details if running locally

    var conf = new SparkConf().setAppName("Distributed ridesharing")
    if (LOCAL) {
      conf = conf.setMaster("local[10]")
    }
    val sc = new SparkContext(conf)

    var path = "./input/"
    if (LOCAL) {
      // NOTE: change this to match your local data path, relative to the sbt package path
      path = "../finalPrj/input/"
    }
    val rides = sc.textFile(path + "cab_rides.csv").map(parseRide).filter(_.price > -1)
    val weather = sc.textFile(path + "weather.csv").map(parseWeather)

    // extremely basic example of using weather data
    weather.collect().foreach(println)
    // rides.collect().foreach(println)
  }
}
