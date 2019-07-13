package com.gilcu2

import com.gilcu2.exploration.Exploration
import com.gilcu2.interfaces._
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.ScallopConf
import com.gilcu2.sparkcollection.DatasetExtension._

object ExplorationMain extends MainTrait {

  def process(configValues0: ConfigValuesTrait, lineArguments0: LineArgumentValuesTrait)(
    implicit spark: SparkSession
  ): Unit = {

    val configValues = configValues0.asInstanceOf[ConfigValues]
    val lineArguments = lineArguments0.asInstanceOf[CommandParameterValues]

    val inputPath = configValues.dataDir + lineArguments.inputName
    val data = Spark.loadCSVFromFile(inputPath)
    data.cache

    data.smartShow("input data")

    val dataSummary = Exploration.summarize(data)
    dataSummary.print(inputPath)

  }

  def getConfigValues(conf: Config): ConfigValuesTrait = {
    val dataDir = conf.getString("DataDir")

    ConfigValues(dataDir)
  }

  def getLineArgumentsValues(args: Array[String], configValues: ConfigValuesTrait): LineArgumentValuesTrait = {

    val parsedArgs = new CommandLineParameterConf(args.filter(_.nonEmpty))
    parsedArgs.verify

    val logCountsAndTimes = parsedArgs.logCountsAndTimes()
    val inputName = parsedArgs.inputName()

    CommandParameterValues(logCountsAndTimes, inputName)
  }

  class CommandLineParameterConf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val logCountsAndTimes = opt[Boolean]()
    val inputName = trailArg[String]()

  }

  case class CommandParameterValues(logCountsAndTimes: Boolean, inputName: String) extends LineArgumentValuesTrait

  case class ConfigValues(dataDir: String) extends ConfigValuesTrait

}