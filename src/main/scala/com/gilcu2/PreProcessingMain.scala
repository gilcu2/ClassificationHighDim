package com.gilcu2

import com.gilcu2.interfaces._
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.rogach.scallop.ScallopConf
import com.gilcu2.datasets.DataFrameExtension._
import com.gilcu2.datasets.DatasetExtension._
import com.gilcu2.datasets.{Json, Svm}
import com.gilcu2.transformers.ColumnSelector
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{MinMaxScaler, VectorAssembler}

object PreProcessingMain extends MainTrait {

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

    val (removerColumnsWithNull, columns) = if (lineArguments.removeNullColumns) {
      val notNullColumns = data.countNullsPerColumn.filter(_._2 == 0).map(_._1).toArray
      val transformer = new ColumnSelector
      transformer.setOutputColumns(notNullColumns)
      (Some(transformer), notNullColumns)
    } else (None, data.columns)

    val scaler = if (lineArguments.scaledFeatures) Some(new MinMaxScaler) else None

    val vectorAssembler = if (lineArguments.vectorAssembling) {
      val transformer = new VectorAssembler
      val featureColumns = (columns.toSet - CLASS_FIELD).toArray
      transformer.setInputCols(featureColumns).setOutputCol(FEATURES_FIELD)
      Some(transformer)
    } else None

    val pipeline = new Pipeline().setStages(Seq(removerColumnsWithNull, scaler,))

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
    val labeledPoints = parsedArgs.vectorAssembling()
    val vectorAssembling = parsedArgs.scaledFeatures()

    CommandParameterValues(logCountsAndTimes, inputName, outputName, removeNullColumns, outputOneFile,
      labeledPoints, vectorAssembling)
  }

  case class CommandParameterValues(logCountsAndTimes: Boolean, inputName: String, outputName: String,
                                    removeNullColumns: Boolean, outputOneFile: Boolean,
                                    vectorAssembling: Boolean, scaledFeatures: Boolean
                                   ) extends LineArgumentValuesTrait

  case class ConfigValues(dataDir: String) extends ConfigValuesTrait

  class CommandLineParameterConf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val inputName = trailArg[String]()
    val outputName = trailArg[String]()

    val logCountsAndTimes = opt[Boolean]()
    val outputOneFile = opt[Boolean]()

    val removeNullColumns = opt[Boolean]()
    val scaledFeatures = opt[Boolean]()
    val vectorAssembling = opt[Boolean]()
  }

}