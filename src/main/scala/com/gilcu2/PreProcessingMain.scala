package com.gilcu2

import com.gilcu2.datasets.DatasetExtension._
import com.gilcu2.datasets.Svm
import com.gilcu2.interfaces._
import com.gilcu2.preprocessing.Preprocessing._
import com.typesafe.config.Config
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.ScallopConf

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

    val (withVectors, vectorPipeline) = makeApplyToVectorPipeline(lineArguments.removeNullColumns, data)

    val (afterVector, afterVectorPipeline) =
      makeApplyAfterVectorPipeline(withVectors, lineArguments.scaledFeatures, lineArguments.maxForCategories)


    val withLabeledPoint = afterVector.toLabeledPoints

    withLabeledPoint.save(outputPath, Svm)

    if (lineArguments.pipelineName.nonEmpty)
      savePipeline(lineArguments.pipelineName, vectorPipeline, afterVectorPipeline, configValues)

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
    val maxForCategories = parsedArgs.maxForCategories()

    val outputOneFile = parsedArgs.outputOneFile()


    CommandParameterValues(logCountsAndTimes, inputName, outputName, removeNullColumns,
      scaledFeatures, maxForCategories,
      outputOneFile, pipeLineName)
  }

  case class CommandParameterValues(logCountsAndTimes: Boolean, inputName: String, outputName: String,
                                    removeNullColumns: Boolean, scaledFeatures: Boolean, maxForCategories: Int,
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
    val maxForCategories = opt[Int](default = Some(1))

    val pipeLineName = opt[String](default = Some(""))

  }


}

