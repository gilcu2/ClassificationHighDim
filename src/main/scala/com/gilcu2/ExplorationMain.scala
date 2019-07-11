package com.gilcu2

import com.gilcu2.exploration.Exploration
import com.gilcu2.interfaces.Spark
import com.gilcu2.interfaces.Time._
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.ScallopConf

object ExplorationMain extends LazyLogging {

  class CommandLineParameterConf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val logCountsAndTimes = opt[Boolean]()
    val inputName = trailArg[String]()
  }

  case class CommandParameterValues(logCountsAndTimes: Boolean, inputName: String)

  case class ConfigValues(dataDir: String)

  def main(implicit args: Array[String]): Unit = {

    val beginTime = getCurrentTime
    logger.info(s"Begin: $beginTime")
    logger.info(s"Arguments: $args")

    println(s"Begin: $beginTime")

    implicit val conf = ConfigFactory.load
    val sparkConf = new SparkConf().setAppName("Exploration")
    implicit val spark = Spark.sparkSession(sparkConf)

    val configValues = getConfigValues(conf)
    val lineArguments = getLineArgumentsValues(args, configValues)

    process(configValues, lineArguments)

    val endTime = getCurrentTime
    val humanTime = getHumanDuration(beginTime, endTime)
    logger.info(s"End: $endTime Total: $humanTime")
    println(s"End: $endTime Total: $humanTime")

  }

  private def process(configValues: ConfigValues, lineArguments: CommandParameterValues)(
    implicit spark: SparkSession
  ): Unit = {

    val inputPath = configValues.dataDir + lineArguments.inputName
    val data = Spark.loadCSVFromFile(inputPath)
    data.show

    val dataSummary = Exploration.summarizeFields(data)
    Exploration.printDataSummary(dataSummary)

  }

  private def getConfigValues(conf: Config): ConfigValues = {
    val dataDir = conf.getString("DataDir")

    ConfigValues(dataDir)
  }

  private def getLineArgumentsValues(args: Array[String], configValues: ConfigValues): CommandParameterValues = {

    val parsedArgs = new CommandLineParameterConf(args.filter(_.nonEmpty))
    parsedArgs.verify

    val logCountsAndTimes = parsedArgs.logCountsAndTimes()
    val inputName = parsedArgs.inputName()

    CommandParameterValues(logCountsAndTimes, inputName)
  }

}