package com.gilcu2.transformers

import com.gilcu2.datasets.DatasetExtension.{CLASS_FIELD, FEATURES_FIELD}
import com.typesafe.scalalogging.LazyLogging
import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.ml.Model
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.VectorUDT
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.DefaultParamsWritable
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}


class VectorAssemblerTransformer(override val uid: String = "VectorAssemblerTransformer") extends
  Model[VectorAssemblerTransformer] with DefaultParamsWritable with LazyLogging {

  final val inputColumns: Param[Array[String]] =
    new Param[Array[String]](this, "inputColumns", "Columns to transform in feature vector")

  final def getInputColumns: Array[String] = $(inputColumns)

  final def setInputColumns(value: Array[String]): VectorAssemblerTransformer = set(inputColumns, value)

  override def transform(dataset: Dataset[_]): DataFrame = {
    logger.info(s"VectorAssemblerTransformer beginning: ${dataset.columns.mkString(",")}")

    val vectorAssembler = new VectorAssembler
    val featureColumns = (dataset.columns.toSet - CLASS_FIELD).toArray
    vectorAssembler.setInputCols(featureColumns).setOutputCol(FEATURES_FIELD)

    val withVectors = vectorAssembler.transform(dataset)

    logger.info("VectorAssemblerTransformer done:")
    withVectors.select(CLASS_FIELD, FEATURES_FIELD)
  }

  override def transformSchema(schema: StructType): StructType = {
    val sourceFields = schema.map(_.name).toSet
    val desiredFields = $(inputColumns).toSet
    require(desiredFields.subsetOf(sourceFields))

    StructType(
      List(
        StructField(CLASS_FIELD, IntegerType, true),
        StructField(FEATURES_FIELD, IntegerType, true)
      )
    )
  }

  override def copy(extra: ParamMap): VectorAssemblerTransformer = defaultCopy(extra)

}
