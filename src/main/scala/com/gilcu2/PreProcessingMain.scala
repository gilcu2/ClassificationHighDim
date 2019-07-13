package com.gilcu2

import com.gilcu2.interfaces._
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.rogach.scallop.ScallopConf
import com.gilcu2.sparkcollection.DataFrameExtension._
import com.gilcu2.sparkcollection.DatasetExtension._
import com.gilcu2.sparkcollection.{Json, Svm}

object PreProcessingMain extends MainTrait {

  case class CommandParameterValues(logCountsAndTimes: Boolean, inputName: String, outputName: String,
                                    removeNullColumns: Boolean, outputOneFile: Boolean,
                                    labeledPoints: Boolean, scaledFeatures: Boolean
                                   ) extends LineArgumentValuesTrait

  case class ConfigValues(dataDir: String) extends ConfigValuesTrait

  class CommandLineParameterConf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val inputName = trailArg[String]()
    val outputName = trailArg[String]()

    val logCountsAndTimes = opt[Boolean]()
    val outputOneFile = opt[Boolean]()
    val labeledPoints = opt[Boolean](short = 'p')

    val removeNullColumns = opt[Boolean]()
    val scaledFeatures = opt[Boolean]()
  }

  def process(configValues0: ConfigValuesTrait, lineArguments0: LineArgumentValuesTrait)(
    implicit spark: SparkSession
  ): Unit = {

    val configValues = configValues0.asInstanceOf[ConfigValues]
    val lineArguments = lineArguments0.asInstanceOf[CommandParameterValues]

    val inputPath = configValues.dataDir + lineArguments.inputName
    val outputPath = configValues.dataDir + lineArguments.outputName

    val data = Spark.loadCSVFromFile(inputPath)
    data.persist
    data.smartShow(inputPath)

    val cleaned = data.transform((df: DataFrame) =>
      if (lineArguments.removeNullColumns) df.rmColumnsWithNull else df
    )

    val withFeaturedVector = cleaned.transform((df: DataFrame) => df.toFeatureVector)

    val withTransformedFeatures = withFeaturedVector.transform((df: DataFrame) =>
      if (lineArguments.scaledFeatures) df.scaleFeatures else df)

    if (lineArguments.labeledPoints) {
      val withLabeledPoints = withTransformedFeatures.transform((ds: DataFrame) => ds.toLabeledPoints)

      withLabeledPoints.save(outputPath, Svm)
    }
    else
      withFeaturedVector.save(outputPath, Json)


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
    val scaledFeatures = parsedArgs.scaledFeatures()

    CommandParameterValues(logCountsAndTimes, inputName, outputName, removeNullColumns, outputOneFile,
      labeledPoints, scaledFeatures)
  }

}