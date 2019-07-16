package com.gilcu2.transformers

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}

class ColumnMapper(override val uid: String = Identifiable.randomUID("ColumnMapper"))
  extends Transformer with DefaultParamsWritable {

  final val columnMapping: Param[String] =
    new Param[String](this, "columnMapping",
      "Mapping of columns, destination None to remove")

  final def getColumnMapping: String = $(columnMapping)

  final def setColumnMapping(value: String): ColumnMapper =
    set(columnMapping, value)

  final def setColumnMapping(value: Map[String, String]): ColumnMapper = {
    val serialized = value.flatMap(pair => Seq(pair._1, pair._2)).mkString(":")
    set(columnMapping, serialized)
  }

  def getMappingMap: Map[String, String] = $(columnMapping).split(":").sliding(2, 2).map(arr => (arr(0), arr(1))).toMap

  override def transform(ds: Dataset[_]): DataFrame = {

    println(s"Columns maping begin: ${ds.columns.mkString(",")}")
    val mapping = getMappingMap

    val toDelete = mapping.filter(_._2.isEmpty).map(_._1).toSet
    val toKeep = ds.columns.filter(!toDelete.contains(_))
    val removedColumnsDS = ds.select(toKeep.head, toKeep.tail: _*)

    val toRenameMap = mapping.filter(_._2.nonEmpty).map(p => (p._1, p._2))
    val renamed = removedColumnsDS.columns.map(name => toRenameMap.getOrElse(name, name))
    val r = removedColumnsDS.toDF(renamed: _*)

    println(s"Columns maping done: ${r.columns.mkString(",")}")

    r
  }

  override def transformSchema(schema: StructType): StructType = {
    val mapping = getMappingMap
    val sourceFields = schema.map(_.name).toSet
    val neededFields = mapping.keySet
    require(neededFields.subsetOf(sourceFields))

    val withRemovedFields = schema.filter(field => mapping.getOrElse(field.name, field.name).nonEmpty)
    val withRenamedFields = withRemovedFields.map(field => {
      val newName = mapping.getOrElse(field.name, field.name)
      field.copy(name = newName)
    })
    StructType(withRenamedFields)
  }

  override def copy(extra: ParamMap): ColumnMapper = defaultCopy(extra)

}
