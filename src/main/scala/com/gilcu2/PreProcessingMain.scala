package com.gilcu2

import com.gilcu2.datasets.DatasetExtension._
import com.gilcu2.datasets.Json
import com.gilcu2.estimators.NullColumnsRemover
import com.gilcu2.interfaces._
import com.gilcu2.transformers.{ColumnMapper, ColumnSelector}
import com.typesafe.config.Config
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{CountVectorizer, MinMaxScaler, VectorAssembler}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.rogach.scallop.ScallopConf

object PreProcessingMain extends MainTrait {

  def process(configValues0: ConfigValuesTrait, lineArguments0: LineArgumentValuesTrait)(
    implicit spark: SparkSession
  ): Unit = {

    val countVectorizer = new CountVectorizer()

    val configValues = configValues0.asInstanceOf[ConfigValues]
    val lineArguments = lineArguments0.asInstanceOf[CommandParameterValues]

    val inputPath = configValues.dataDir + lineArguments.inputName
    val outputPath = configValues.dataDir + lineArguments.outputName

    val data = Spark.loadCSVFromFile(inputPath)
    data.persist
    data.smartShow(inputPath)

    val nullColumnsRemover = makeNullColumnsRemoverStage(lineArguments.removeNullColumns)
    val (vectorAssembler, selectorAferAssembler) = makeVectorAssemblerStage(lineArguments)
    val (scaler, afterScalerMapper) = makeScalerStage(lineArguments.vectorAssembling,
      lineArguments.scaledFeatures)

    val stages = Array(removerColumnsWithNull, vectorAssembler, selectorAferAssembler, scaler,
      afterScalerMapper).filter(_.nonEmpty).map(_.get)
    val pipeline = new Pipeline().setStages(stages)

    val model = pipeline.fit(data)

    val processed = model.transform(data)

    processed.save(outputPath, Json)


  }

  def makeScalerStage(vectorAssembling: Boolean, scaling: Boolean): (Option[MinMaxScaler], Option[ColumnMapper]) =
    if (vectorAssembling && scaling) {
      val tempColumn = FEATURES_FIELD + "Scaled"

      val scaler = new MinMaxScaler
      scaler.setInputCol(FEATURES_FIELD)
      scaler.setOutputCol(tempColumn)

      val mapper = new ColumnMapper
      mapper.setColumnMapping(Map(FEATURES_FIELD -> None, tempColumn -> Some(FEATURES_FIELD)))

      (Some(scaler), Some(mapper))
    } else (None, None)

  def makeVectorAssemblerStage(vectorAssembling: Boolean)
  : (Option[VectorAssembler], Option[ColumnSelector]) =
    if (vectorAssembling) {
      val vectorAssembler = new VectorAssembler
      val featureColumns = (columns.toSet - CLASS_FIELD).toArray
      vectorAssembler.setInputCols(featureColumns).setOutputCol(FEATURES_FIELD)

      val selecter = new ColumnSelector
      val desiredColumns = Array(CLASS_FIELD, FEATURES_FIELD)
      selecter.setOutputColumns(desiredColumns)

      (Some(vectorAssembler), Some(selecter))
    } else (None, None)


  def makeNullColumnsRemoverStage(removeNullColumns: Boolean)
  : (Option[NullColumnsRemover]) =
    if (removeNullColumns) Some(new NullColumnsRemover()) else None

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
  val vectorAssembling = opt[Boolean]()
  val scaledFeatures = opt[Boolean]()

}