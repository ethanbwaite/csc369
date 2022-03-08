package rides

import org.apache.spark.SparkContext._
import scala.io._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection._
import scala.math._

import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.Instant

case class Ride (
  val distance: Float,
  val cabType: String,
  val day: String,
  val hour: Int,
  val destination: String,
  val source: String,
  val price: Double,
  val surgeMultiplier: Double,
  val name: String
)

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
)

object App {
  val DEBUG = true

  def weightedEuclideanDistance(values1: List[Any], values2: List[Any]): Double = {
    val sumOfSquaredDifferences = (values1 zip values2).map({
      case (v1: Int, v2: Int) => pow(v1 - v2, 2).toDouble
      case (v1: Double, v2: Double) => pow(v1 - v2, 2).toDouble
      case (v1: Float, v2: Float) => pow(v1 - v2, 2).toDouble
      case _ => 0.0
    }).sum
    val numericFieldCount = values1.map({
      case v: Int => 1
      case v: Double => 1
      case v: Float => 1
      case _ => 0
    }).reduce((x, y) => x+y)

    // numerical dist = sqrt(sum( (xi - yi)^2 ))
    // wgtd numerical dist = dist * (# numerical attrbs / total len)
    // TODO: standardize euclidean distance by range or z-score, else standardize all data to begin with
    val dist = sqrt(sumOfSquaredDifferences) * (numericFieldCount.toDouble / values1.length.toDouble)

    if (DEBUG) {
      println("SSE = " + f"${sumOfSquaredDifferences}%.2f" + s" for ${numericFieldCount} elements --> dist = " + f"${dist}%.2f")
    }
    return dist
  }

  def weightedCategoricalDistance(values1: List[Any], values2: List[Any]): Double = {
    // count categorical attribute mismatches
    val numberOfMismatches = (values1 zip values2).map({
      case (v1: String, v2: String) => if (v1 != v2) 1 else 0
      case _ => 0
    }).sum
    // count categorical attributes
    val categoricalFieldCount = values1.map({
      case v: String => 1
      case _ => 0
    }).reduce((x, y) => x+y)

    // categorical dist = # mismatches / # categorical attrbs = % mismatch (not x100% though)
    // wgtd categorical dist = dist * (# categorical attrbs / total len)
    val dist = numberOfMismatches / categoricalFieldCount.toDouble
    val wgtdDist = dist * (categoricalFieldCount.toDouble / values1.length.toDouble)

    if (DEBUG) {
      println(s"Mismatches: ${numberOfMismatches} / ${categoricalFieldCount} --> dist = " + f"${wgtdDist}%.2f")
    }

    return wgtdDist
  }

  def getDistance(row1: Product, row2: Product): Double = {
    // Calculates total distance based on all attributes in the Row object.
    // Considers all Int, Double, Float as numeric
    // Considers all Strings as categorical
    val values1 = row1.productIterator.toList
    val values2 = row2.productIterator.toList
    return weightedEuclideanDistance(values1, values2) + weightedCategoricalDistance(values1, values2)
  }

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

    val conf = new SparkConf().setAppName("Distributed ridesharing")
    val sc = new SparkContext(conf)

    val rides = sc.textFile("./input/cab_rides.csv").map(parseRide).filter(_.price > -1)
    val weather = sc.textFile("./input/weather.csv").map(parseWeather)

    // extremely basic example of using weather data
    val r1 = rides.take(1)(0)
    val r2 = rides.take(2)(1)
    println(r1)
    println(r2)
    println(getDistance(r1, r2))

    // weather.collect().foreach(println)
    // rides.collect().foreach(println)
  }
}
