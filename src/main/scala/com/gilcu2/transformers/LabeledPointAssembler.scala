package com.gilcu2.transformers

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}
import com.gilcu2.datasets.DatasetExtension._


//class LabeledPointAssembler(override val uid: String = "LabeledPointAssembler") extends Transformer {
//
//  override def transform(ds: Dataset[_]): DataFrame = ds.toFeatureVector
//
//  override def transformSchema(schema: StructType): StructType = {
//    val sourceFields = schema.map(_.name).toSet
//    val desiredFields = $(outputColumns).toSet
//    require(desiredFields.subsetOf(sourceFields))
//
//    StructType(schema.filter(field => desiredFields.contains(field.name)))
//  }
//
//  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
//
//}
