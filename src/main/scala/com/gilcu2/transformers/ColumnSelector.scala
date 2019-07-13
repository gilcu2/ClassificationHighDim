package com.gilcu2.transformers

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}


class ColumnSelector(override val uid: String = "ColumnSelector") extends Transformer {

  final val outputColumns: Param[Array[String]] =
    new Param[Array[String]](this, "outputColumns", "Columns to left")

  final def getOutputColumns: Array[String] = $(outputColumns)

  final def setOutputColumns(value: Array[String]): ColumnSelector = set(outputColumns, value)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val columns = $(outputColumns)
    dataset.select(columns.head, columns.tail: _*)
  }

  override def transformSchema(schema: StructType): StructType = {
    val sourceFields = schema.map(_.name).toSet
    val desiredFields = $(outputColumns).toSet
    require(desiredFields.subsetOf(sourceFields))

    StructType(schema.filter(field => desiredFields.contains(field.name)))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

}
