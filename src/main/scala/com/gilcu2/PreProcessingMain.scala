package com.gilcu2

import com.gilcu2.interfaces._
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.rogach.scallop.ScallopConf
import DataFrame._
import Dataset._

object PreProcessingMain extends MainTrait {

  class CommandLineParameterConf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val inputName = trailArg[String]()
    val outputName = trailArg[String]()

    val logCountsAndTimes = opt[Boolean]()
    val outputOneFile = opt[Boolean]()
    val labeledPoints = opt[Boolean](short = 'p')

    val removeNullColumns = opt[Boolean]()
  }

  def process(configValues0: ConfigValuesTrait, lineArguments0: LineArgumentValuesTrait)(
    implicit spark: SparkSession
  ): Unit = {

    val configValues = configValues0.asInstanceOf[ConfigValues]
    val lineArguments = lineArguments0.asInstanceOf[CommandParameterValues]

    val inputPath = configValues.dataDir + lineArguments.inputName + ".csv"

    val data = Spark.loadCSVFromFile(inputPath)
    data.cache
    data.smartShow()

    val cleaned = clean(data, lineArguments.removeNullColumns)

    println("\nCleaned")
    cleaned.smartShow()

    val withFeaturedVector = cleaned.toFeatureVector
    println("\nwithFeaturedVector")
    withFeaturedVector.show

    if (lineArguments.labeledPoints) {
      val withLabeledPoints = withFeaturedVector.toLabeledPoints
      val outputPath = configValues.dataDir + lineArguments.outputName + ".svm"
      withLabeledPoints.saveSVM(outputPath)
    }
    else {
      val outputPath = configValues.dataDir + lineArguments.outputName + ".json"
      withFeaturedVector.saveJson(outputPath)
    }

  }

  def clean(df: DataFrame, removeNullColumns: Boolean): DataFrame = {

    if (removeNullColumns)
      df.rmColumnsWithNull
    else
      df
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
    val outputName = parsedArgs.outputName()
    val removeNullColumns = parsedArgs.removeNullColumns()
    val outputOneFile = parsedArgs.outputOneFile()
    val labeledPoints = parsedArgs.labeledPoints()

    CommandParameterValues(logCountsAndTimes, inputName, outputName, removeNullColumns, outputOneFile, labeledPoints)
  }



  case class CommandParameterValues(logCountsAndTimes: Boolean, inputName: String, outputName: String,
                                    removeNullColumns: Boolean, outputOneFile: Boolean,
                                    labeledPoints: Boolean
                                   ) extends LineArgumentValuesTrait

  case class ConfigValues(dataDir: String) extends ConfigValuesTrait

}