package com.gilcu2.transformers

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}

class ColumnMapper(override val uid: String = "ColumnManaging") extends Transformer {

  final val columnMapping: Param[Map[String, Option[String]]] =
    new Param[Map[String, Option[String]]](this, "columnMapping",
      "Mapping of columns, destination None to delete")

  final def getColumnMapping: Map[String, Option[String]] = $(columnMapping)

  final def setColumnMapping(value: Map[String, Option[String]]): ColumnMapper =
    set(columnMapping, value)

  override def transform(ds: Dataset[_]): DataFrame = {
    val mapping = $(columnMapping)

    val toDelete = mapping.filter(_._2.isEmpty).map(_._1).toSet
    val toKeep = ds.columns.filter(toDelete.contains)
    val removedColumnsDS = ds.select(toKeep.head, toKeep.tail: _*)

    val toRenameMap = mapping.filter(_._2.nonEmpty).map(p => (p._1, p._2.get))
    val renamed = removedColumnsDS.columns.map(name => toRenameMap.getOrElse(name, name))
    removedColumnsDS.toDF(renamed: _*)

  }

  override def transformSchema(schema: StructType): StructType = {
    val mapping = $(columnMapping)
    val sourceFields = schema.map(_.name).toSet
    val desiredFields = $(columnMapping).map(_._1).toSet
    require(desiredFields.subsetOf(sourceFields))

    StructType(schema.filter(field => mapping(field.name).isDefined))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

}
