package testUtil

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {

  val TIME_MS = "10000000"
  val SPARK_PROCESSORS = "1"

  val sparkConf = new SparkConf().setAppName("PhoneLogProcessing test").set("spark.network.timeout", TIME_MS)

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .config(sparkConf)
      .master(s"local[$SPARK_PROCESSORS]")
      .appName("spark test")
      .getOrCreate()
  }

}
