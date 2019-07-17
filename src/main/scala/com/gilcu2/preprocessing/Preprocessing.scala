package com.gilcu2.preprocessing

import com.gilcu2.PreProcessingMain.CommandParameterValues
import com.gilcu2.datasets.DatasetExtension.FEATURES_FIELD
import com.gilcu2.estimators.{NullColumnsRemover, VectorAssemblerEstimator}
import com.gilcu2.transformers.ColumnMapper
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{MinMaxScaler, VectorIndexer}
import org.apache.spark.sql.DataFrame

object Preprocessing {

  def makeApplyToVectorPipeline(removeNullColumns: Boolean, data: DataFrame): (DataFrame, Pipeline) = {
    val nullColumnsRemover = makeNullColumnsRemoverStage(removeNullColumns)
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

  def makeApplyAfterVectorPipeline(withVectors: DataFrame, scale: Boolean, maxForCategories: Int)
  : (DataFrame, Option[Pipeline]) = {

    val (indexer, afterIndexerMapper) = if (maxForCategories > 1) makeScalerStage else (None, None)

    val (scaler, afterScalerMapper) = if (scale) makeScalerStage else (None, None)

    val stages = Array(indexer, afterIndexerMapper, scaler, afterScalerMapper).filter(_.nonEmpty).map(_.get)
    val pipeline = new Pipeline().setStages(stages)

    val model = pipeline.fit(withVectors)

    val processed = model.transform(withVectors)
    (processed, Some(pipeline))
  }


  def makeScalerStage: (Option[MinMaxScaler], Option[ColumnMapper]) = {
    val tempColumn = FEATURES_FIELD + "Scaled"

    val scaler = new MinMaxScaler
    scaler.setInputCol(FEATURES_FIELD)
    scaler.setOutputCol(tempColumn)

    val mapper = new ColumnMapper
    mapper.setColumnMapping(Map(FEATURES_FIELD -> "", tempColumn -> FEATURES_FIELD))

    (Some(scaler), Some(mapper))
  }

  def makeIndexerStage(maxForCategories: Int): (Option[VectorIndexer], Option[ColumnMapper]) = {
    val tempColumn = FEATURES_FIELD + "Indexed"

    val indexer = new VectorIndexer()
      .setInputCol(FEATURES_FIELD)
      .setOutputCol(tempColumn)
      .setMaxCategories(maxForCategories)

    val mapper = new ColumnMapper
    mapper.setColumnMapping(Map(FEATURES_FIELD -> "", tempColumn -> FEATURES_FIELD))

    (Some(indexer), Some(mapper))
  }


}
