package com.gilcu2

import com.gilcu2.datasets.DatasetExtension._
import com.gilcu2.datasets.{Json, Svm}
import com.gilcu2.estimators.{NullColumnsRemover, VectorAssemblerEstimator}
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

    val (withVectors, vectorPipeline) = toVectors(lineArguments, data)

    val (scaled, scalerPipeline) = if
    (lineArguments.scaledFeatures) scaleVector(withVectors)
    else (withVectors, None)

    val withLabeledPoint = scaled.toLabeledPoints

    withLabeledPoint.save(outputPath, Svm)

    if (lineArguments.pipelineName.nonEmpty)
      savePipeline(lineArguments.pipelineName, vectorPipeline, scalerPipeline, configValues)

  }

  def savePipeline(pipelineName: String, vectorPipeline: Pipeline, scalerPipeline: Option[Pipeline],
                   config: ConfigValues): Unit = {

    val vectorPipelinePath = s"${config.pipelinesDir}$pipelineName-vector.pipeline"
    vectorPipeline.write.overwrite().save(vectorPipelinePath)

    if (scalerPipeline.nonEmpty) {
      val scalerPipelinePath = s"${config.pipelinesDir}$pipelineName-scaled.pipeline"
      scalerPipeline.get.write.overwrite().save(scalerPipelinePath)
    }
  }

  private def scaleVector(withVectors: DataFrame): (DataFrame, Option[Pipeline]) = {
    val (scaler, afterScalerMapper) = makeScalerStage

    val pipeline = new Pipeline().setStages(Array(scaler, afterScalerMapper))

    val model = pipeline.fit(withVectors)

    val processed = model.transform(withVectors)
    (processed, Some(pipeline))
  }

  def toVectors(lineArguments: CommandParameterValues, data: DataFrame): (DataFrame, Pipeline) = {
    val nullColumnsRemover = makeNullColumnsRemoverStage(lineArguments.removeNullColumns)
    val vectorAssembler = makeVectorAssemblerStage

    val stages = Array(nullColumnsRemover, vectorAssembler).filter(_.nonEmpty).map(_.get)
    val pipeline = new Pipeline().setStages(stages)

    val model = pipeline.fit(data)

    val processed = model.transform(data)
    (processed, pipeline)
  }

  def makeVectorAssemblerStage: Option[VectorAssemblerEstimator] = Some(new VectorAssemblerEstimator)

  def makeNullColumnsRemoverStage(removeNullColumns: Boolean): (Option[NullColumnsRemover]) =
    if (removeNullColumns) Some(new NullColumnsRemover()) else None

  def makeScalerStage: (MinMaxScaler, ColumnMapper) = {
      val tempColumn = FEATURES_FIELD + "Scaled"

      val scaler = new MinMaxScaler
      scaler.setInputCol(FEATURES_FIELD)
      scaler.setOutputCol(tempColumn)

      val mapper = new ColumnMapper
    mapper.setColumnMapping(Map(FEATURES_FIELD -> "", tempColumn -> FEATURES_FIELD))

    (scaler, mapper)
  }

  def getConfigValues(conf: Config): ConfigValuesTrait = {
    val dataDir = conf.getString("DataDir")
    val pipelineDir = conf.getString("PipelineDir")

    ConfigValues(dataDir, pipelineDir)
  }

  def getLineArgumentsValues(args: Array[String], configValues: ConfigValuesTrait): LineArgumentValuesTrait = {

    val parsedArgs = new CommandLineParameterConf(args.filter(_.nonEmpty))
    parsedArgs.verify

    val logCountsAndTimes = parsedArgs.logCountsAndTimes()
    val inputName = parsedArgs.inputName()
    val outputName = parsedArgs.outputName()
    val pipeLineName = parsedArgs.pipeLineName()

    val removeNullColumns = parsedArgs.removeNullColumns()
    val scaledFeatures = parsedArgs.scaledFeatures()
    val outputOneFile = parsedArgs.outputOneFile()


    CommandParameterValues(logCountsAndTimes, inputName, outputName, removeNullColumns, scaledFeatures,
      outputOneFile, pipeLineName)
  }

  case class CommandParameterValues(logCountsAndTimes: Boolean, inputName: String, outputName: String,
                                    removeNullColumns: Boolean, scaledFeatures: Boolean,
                                    outputOneFile: Boolean, pipelineName: String
                                   ) extends LineArgumentValuesTrait

  case class ConfigValues(dataDir: String, pipelinesDir: String) extends ConfigValuesTrait

  class CommandLineParameterConf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val inputName = trailArg[String]()
    val outputName = trailArg[String]()

    val logCountsAndTimes = opt[Boolean]()
    val outputOneFile = opt[Boolean]()

    val removeNullColumns = opt[Boolean]()
    val scaledFeatures = opt[Boolean]()

    val pipeLineName = opt[String](default = Some(""))

  }


}

