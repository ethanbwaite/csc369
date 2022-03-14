package rides

import org.apache.spark.SparkContext._
import scala.io._
import org.apache.spark.{ HashPartitioner, SparkConf, SparkContext }
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection._
import scala.math._
import org.apache.spark.mllib.rdd.MLPairRDDFunctions.fromPairRDD

import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.Instant

case class Ride (
  val id: String,
  val distance: Float,
  val cabType: String,
  val day: String,
  val hour: Int,
  val hourHalf: Int,
  val destination: String,
  val source: String,
  val price: Double,
  val surgeMultiplier: Double,
  val name: String
)

case class Weather (
  val temp: Float,
  val location: String,
  val clouds: Float,
  val pressure: Float,
  val rain: Float,
  val day: String,
  val hour: Int,
  val hourHalf: Int,
  val humidity: Float,
  val wind: Float
)

case class Record (
// Ride attributes
  val distance: Float,
  val cabType: String,
  val day: String,
  val hour: Int,
  val destination: String,
  val source: String,
  val surgeMultiplier: Double,
  val rideName: String,
// Weather attributes
  val temp: Float,
  val clouds: Float,
  val pressure: Float,
  val rain: Float,
  val humidity: Float,
  val wind: Float
)

// stores a Record for data record distance comparison, label or "class value" is price
case class LabeledRecord(id: String, r: Record, label: Double) {
  var prediction: Double = -0.1   // set originally to negative value to indicate error (unset yet)

  def setPrediction(pred: Double): Unit = {
    prediction = pred
  }

  def getError(): Double = {
    // this is the error case (i.e. no real prediction stored yet, so pass along the error negative value
    if (prediction < 0) {
      return prediction
    }
    // return the real error between prediction and true price
    return abs(label - prediction)
  }
}

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
    }).sum

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
    }).sum

    // categorical dist = # mismatches / # categorical attrbs = % mismatch (not x100% though)
    // wgtd categorical dist = dist * (# categorical attrbs / total len)
    // simplified: wgtd categorical dist = # mismatches / total len
    val dist = numberOfMismatches / values1.length.toDouble

    if (DEBUG) {
      println(s"Mismatches: ${numberOfMismatches} / ${categoricalFieldCount} --> dist = " + f"${dist}%.2f")
    }

    return dist
  }

  def getDistance(row1: Product, row2: Product): Double = {
    // Calculates total distance based on all attributes in the Row object.
    // Considers all Int, Double, Float as numeric
    // Considers all Strings as categorical
    val values1 = row1.productIterator.toList
    val values2 = row2.productIterator.toList
    if (DEBUG) {
      println(s"Record 1: $values1")
      println(s"Record 2: $values2")
    }
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

  def timestampToDayHourHalf(timestamp: Long): (String, Int, Int) = {
    // ZonedDateTime object in Boston time (UTC-5)
    // https://docs.oracle.com/javase/8/docs/api/java/time/ZonedDateTime.html
    val dt = ZonedDateTime.ofInstant(Instant.ofEpochSecond(timestamp), ZoneId.of("UTC-5"))
    val min = dt.getMinute
    // compare if in first or second half of the hour (since weather msts take at 15mins and 45mins)
    val halfOfHour = 1
    if (min >= 30) {
      val halfOfHour = 2
    }
    return (dt.getDayOfWeek.toString, dt.getHour, halfOfHour)
  }

  def parseRide(in: String): Ride = {
    val s = in.split(",")

    val distance = floatOrZero(s(0))
    val cabType = s(1)
    val time = timestampToDayHourHalf(s(2).toLong)
    val destination = s(3)
    val source = s(4)
    val price = doubleOrDefault(s(5), -1.0)
    val surge = doubleOrDefault(s(6), 1.0)
    val id = s(7)
    val name = s(9)

    val ride = Ride(id, distance, cabType, time._1, time._2, time._3, destination, source, price, surge, name)
    return ride
  }

  def parseWeather(in: String): Weather = {
    val s = in.split(",")

    val temp = floatOrZero(s(0))
    val location = s(1)
    val clouds = floatOrZero(s(2))
    val pressure = floatOrZero(s(3))
    val rain = floatOrZero(s(4))
    val time = timestampToDayHourHalf(s(5).toLong)
    val humidity = floatOrZero(s(6))
    val wind = floatOrZero(s(7))

    val weather = Weather(temp, location, clouds, pressure, rain, time._1, time._2, time._3, humidity, wind)
    return weather
  }

  def standardize(scores: List[Double]): List[Double] = {

    val sumCount = scores.map(x => (x, 1)).reduce((x,y) => (x._1 + y._1, x._2 + y._2))
    val mean = sumCount._1 / sumCount._2
    val devs = scores.map(score => (score - mean) * (score - mean))
    val stddev = Math.sqrt(devs.sum / sumCount._2)
    return scores.map(x => (x - mean)/stddev)


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

    // DATA SETUP  ------------------------------------------------------------------------------------------------------------------------

    var path = "./input/"
    if (LOCAL) {
      // NOTE: change this to match your local data path, relative to the sbt package path
      path = "../finalPrj/input/"
    }
    val rides = sc.textFile(path + "cab_rides.csv").map(parseRide).filter(_.price > -1)
    val weather = sc.textFile(path + "weather.csv").map(parseWeather).
      keyBy((w => (w.location, w.day, w.hour, w.hourHalf))).  // key by future key (time and place)
      reduceByKey((w1, w2) => w1).                            // keep only 1 weather reading per time
      map({case (k, v) => v})                                 // restore data as Weather objects only, not (k,v) pairs

    // join ride and weather data on time and place, then store combined data in a LabeledRecord (label = price)
    val joined = rides.keyBy(x => (x.day, x.hour, x.hourHalf, x.source))
      .join(weather.keyBy(x => (x.day, x.hour, x.hourHalf, x.location)))
      .map({
        case (k, (ride, weather)) => ((k._1, k._2, k._3, k._4), LabeledRecord(
          ride.id,  // id for bookkeeping
          Record(   // data to use for dist computation
            ride.distance,
            ride.cabType,
            ride.day,
            ride.hour,
            ride.destination,
            ride.source,
            ride.surgeMultiplier,
            ride.name,
            weather.temp,
            weather.clouds,
            weather.pressure,
            weather.rain,
            weather.humidity,
            weather.wind
          ),
          ride.price) // label = price
        )
      })

    // KNN FUNCTION SETUP  ------------------------------------------------------------------------------------------------------------------------
    val PERCENT_OF_DATA = 0.01
    val TRAIN_PERCENT = 0.8
    val K = 10
    val RESULTS_TO_DISPLAY = 4

    // split into train and test (TODO: and validation?)
    val recordSize = joined.count()
    val subsetSize = (recordSize * PERCENT_OF_DATA).toInt
    val joinedSubset = sc.parallelize(joined.take(subsetSize)) // Smaller sample size for development
    val numTrain = (subsetSize * TRAIN_PERCENT).toInt    // number of records to include in training set
    val numTest = (subsetSize - numTrain).toInt
    val k = min(K, numTrain).toInt

    // take test set (rather than train set) first bc take() stores all in main mem, so keep amount small; take random sample (without replacement)
    val test = sc.parallelize(joinedSubset.takeSample(false, (numTest)))
    val train = joinedSubset.subtract(test).partitionBy( new HashPartitioner(5)) // train set is everything not in the test set

    println("\n--- Train and test split. ---")
    println(f"$subsetSize/$recordSize records used (${PERCENT_OF_DATA * 100}%.2f%%)")
    println(f"\t${TRAIN_PERCENT * 100}%.2f%%" + s" train\t($numTrain records)")
    println(f"\t${(1 - TRAIN_PERCENT) * 100}%.2f%%" + s" test\t($numTest records)")
    println(f"K = ${k}")

    // KNN CALCULATION  ------------------------------------------------------------------------------------------------------------------------
    // for each item in the test set, calculate the avg predicted price from the prices of the k nearest neighbors


    /*** Distance matrix calculation (between test and training records ***/
    val t0_dist = System.currentTimeMillis()

    // TODO partition on distMatrix or kthDists
    // rTest * rTrain -> (rTest_id, (rTest_price, rTrain_price, dist))
    val distMatrix = test.cartesian(train)
      .map({ case (r1, r2) => (r1._2.id, (r1._2.label, r2._2.label, getDistance(r1._2.r, r2._2.r)))})

    println("\n--- Distances calculated. ---")
    println(" *ride*\t\t*actual*\t*training*\t*distance*")
    distMatrix.take(RESULTS_TO_DISPLAY)
      .foreach(x => println(x._1.split("-")(0)
        + "\t $" + (math rint x._2._1 * 100) / 100
        + "\t\t $" + (math rint x._2._2 * 100) / 100
        + "\t\t " + (math rint x._2._3 * 10000) / 10000
        ))
    val t1_dist = System.currentTimeMillis()
    println(" > " + (t1_dist - t0_dist) + "ms elapsed")


    /*** Distance of Kth nearest neighbor for each ride in the test set ***/
    val t0_k_dist = System.currentTimeMillis()
    val kthDists = distMatrix.topByKey(k)(Ordering[Double].reverse.on(_._3))
      .mapValues(x => x.maxBy(_._3)._3)

    println("\n--- Max dists for each test record found. ---")
    println(" *ride*\t\t*distance*")
    kthDists.take(RESULTS_TO_DISPLAY)
      .foreach(x => println(x._1.split("-")(0) + "\t " + (math rint x._2 * 10000) / 10000))
    val t1_k_dist = System.currentTimeMillis()
    println(" > " + (t1_k_dist - t0_k_dist) + "ms elapsed")


    /*** Find prices rTest's k nearest neighbors ***/
    val t0_k_nearest = System.currentTimeMillis()
    // (rTest_id, (rTest_price, rTrain_price, dist)) * (rTest_id, maxDist) -> ((rTest_id, rTest_price), rTrain_price)
    val kNearestNeighbors = distMatrix.join(kthDists)
      .filter({case (rTest_id, ((rTest_price, rTrain_price, dist) , maxDist)) => (maxDist >= dist)})  // keep only K nearest
      .map({case (rTest_id, ((rTest_price, rTrain_price, dist) , maxDist)) => ((rTest_id, rTest_price), rTrain_price)})

    println("\n--- k nearest neighbors for each test record found. ---")
    println(" *ride*\t\t*actual*\t*neighbor*")
    kNearestNeighbors.take(RESULTS_TO_DISPLAY)
      .foreach(x => println(x._1._1.split("-")(0) + "\t $" + x._1._2 + "\t\t $" + x._2))
    val t1_k_nearest = System.currentTimeMillis()
    println(" > " + (t1_k_nearest - t0_k_nearest) + "ms elapsed")

    /*** Compute average neighbor price for each rTest ***/
    val t0_pred = System.currentTimeMillis()
    // ((rTest_id, rTest_price), rTrain_price) -> (rTest_id, (rTest_price, avgTrain_price))
    val testPredictions = kNearestNeighbors.combineByKey(
      v => (v, 1),  // set init accum = (sum, count) to (first train price, 1)
      (acc: (Double, Int), v) => (acc._1 + v, acc._2 + 1),  // add next train price; incr count by 1
      (acc1: (Double, Int), acc2: (Double, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)  // accum the accums
    ).map({ case (rTest, value) => (rTest._1, ((math rint 100 * rTest._2) / 100, (math rint (value._1 * 100.0 / value._2)) / 100)) })

    println("\n--- Price predictions made. ---")
    println(" *ride*\t\t*actual*\t*prediction*")
    testPredictions.take(RESULTS_TO_DISPLAY)
      .foreach(x => println(x._1.split("-")(0) + "\t $" + x._2._1 + "\t\t $" + x._2._2))
    val t1_pred = System.currentTimeMillis()
    println(" > " + (t1_pred - t0_pred) + "ms elapsed")

    /*** Evaluate predictions (the average error) ***/
    val errInfo = testPredictions.map({ case (id, (real, pred)) => abs(real - pred) })  // rdd of errors
      .aggregate(0.0, 0) ((acc, newErr) => (acc._1 + newErr, acc._2 + 1),  // sum and count
                          (x,y) => (x._1 + y._1, x._2 + y._2)) // add the sums and counts
    val avgError = errInfo._1 / errInfo._2

    println("\n--- Avg Error = " + f"$$$avgError%.2f" + " ---\n")
  }
}
