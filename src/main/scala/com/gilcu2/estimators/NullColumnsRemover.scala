package com.gilcu2.estimators

import com.gilcu2.datasets.DatasetExtension._
import com.gilcu2.transformers.ColumnSelector
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.StructType

class NullColumnsRemover(override val uid: String = Identifiable.randomUID("NullColumnsRemover"))
  extends Estimator[ColumnSelector] {

  override def fit(dataset: Dataset[_]): ColumnSelector = {
    println(s"NullColumnsRemover beginning: ${dataset.columns.mkString(",")}")

    val notNullColumns = dataset.countNullsPerColumn.filter(_._2 == 0).map(_._1).toArray

    val model = new ColumnSelector
    model.setOutputColumns(notNullColumns)

    println(s"NullColumnsRemover fitted: ${notNullColumns}")

    model
  }

  override def transformSchema(schema: StructType): StructType = schema

  override def copy(extra: ParamMap): NullColumnsRemover = defaultCopy(extra)

}
