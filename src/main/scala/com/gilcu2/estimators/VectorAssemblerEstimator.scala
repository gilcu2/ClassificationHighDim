package com.gilcu2.estimators

import com.gilcu2.datasets.DatasetExtension._
import com.gilcu2.interfaces.Time
import com.gilcu2.transformers.{ColumnSelector, VectorAssemblerTransformer}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.StructType

class VectorAssemblerEstimator(override val uid: String = Identifiable.randomUID("VectorAssemblerEstimator"))
  extends Estimator[VectorAssemblerTransformer] with DefaultParamsWritable with LazyLogging {

  override def fit(dataset: Dataset[_]): VectorAssemblerTransformer = {
    logger.info(s"VectorAssemblerEstimator beginning: ${dataset.columns.mkString(",")}")

    val vectorAssembler = new VectorAssemblerTransformer
    val featureColumns = (dataset.columns.toSet - CLASS_FIELD).toArray
    vectorAssembler.setInputColumns(featureColumns)

    logger.info(s"VectorAssemblerEstimator end ${Time.getCurrentTime}")
    vectorAssembler
  }

  override def transformSchema(schema: StructType): StructType = schema

  override def copy(extra: ParamMap): VectorAssemblerEstimator = defaultCopy(extra)

}
