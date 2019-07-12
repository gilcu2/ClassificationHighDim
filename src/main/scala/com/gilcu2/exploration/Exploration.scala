package com.gilcu2.exploration

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.gilcu2.interfaces.DataFrame._

case class DataSummary(rowNumber: Long, columnNumber: Int, fields: Seq[String],
                       booleanFields: Seq[String], integerFields: Seq[String],
                       realFields: Seq[String], otherFields: Seq[String],
                       fieldsSummary: Seq[FieldSummary], classesSize: Seq[(Int, Long)],
                       nullCountPerColumn: Seq[(String, Long)], nullRows: Long, totalZeros: Long
                      )

case class FieldSummary(name: String, min: String, max: String)

object Exploration extends LazyLogging {

  val dotCeroRegex = "([0-9]+).0".r
  val integerRegex = "^(-?[0-9]+)$".r
  val realRegex = "^(-?[0-9]+.[0-9]+)$".r

  def summarize(df: DataFrame): DataSummary = {

    val size = df.count
    val fieldNames = df.columns
    val dim = fieldNames.length

    val fieldsSummary = computeFieldsSummary(df, dim, fieldNames)
    val fieldTypes = fieldsSummary.map(summary => computeFieldType(summary))

    val booleanFields = fieldTypes.filter(_._2 == 'B').map(_._1.name)
    val integerFields = fieldTypes.filter(_._2 == 'I').map(_._1.name)
    val realFields = fieldTypes.filter(_._2 == 'R').map(_._1.name)
    val otherFields = fieldTypes.filter(_._2 == 'O').map(_._1.name)

    val classesSize = computeClassesSizes(df)

    val nullsPerColumn = df.countNullsPerColumn
    val nullRows = df.countRowsWithNullOrEmptyString

    val totalZeros = df.countNumberOfZeros

    DataSummary(size, dim, fieldNames, booleanFields, integerFields, realFields, otherFields,
      fieldsSummary, classesSize, nullsPerColumn, nullRows, totalZeros)

  }

  def printDataSummary(dataSummary: DataSummary, inputPath: String): Unit = {

    def printFieldTypes(fieldType: String, fields: Seq[String]): Unit =
      println(s"Fields $fieldType: ${fields.size}\n ${fields}\n")

    println("\nClassifier report\n")

    println(s"Input: $inputPath\n")

    println(s"Size: ${dataSummary.rowNumber}\n")
    println(s"Fields: ${dataSummary.fields.size}\n ${dataSummary.fields}\n")

    printFieldTypes("Boolean", dataSummary.booleanFields)
    printFieldTypes("Integer", dataSummary.integerFields)
    printFieldTypes("Real", dataSummary.realFields)
    printFieldTypes("Other", dataSummary.otherFields)

    println(s"\nRows with null: ${dataSummary.nullRows}")

    val columnsWithNulls = dataSummary.nullCountPerColumn.filter(_._2 > 0)
    val countColumnsWithNulls = columnsWithNulls.length
    println(s"\nColumns with nulls: $countColumnsWithNulls}")
    println("Nulls per column")
    columnsWithNulls.foreach(pair => println(s"${pair._1}\t${pair._2}"))

    val zerosFraction = dataSummary.totalZeros.toDouble / (dataSummary.rowNumber * dataSummary.columnNumber)
    println(s"\nTotal number of zeros: ${dataSummary.totalZeros} ${zerosFraction}")

    if (dataSummary.classesSize.nonEmpty) {
      println(s"\nClasses: ${dataSummary.classesSize.size}")
      dataSummary.classesSize.foreach(pair => println(s"${pair._1}\t${pair._2}"))
    }

    println("\nFields summary")
    println("Name\tMin\tMax")
    dataSummary.fieldsSummary.foreach(summary =>
      println(s"${summary.name}\t${summary.min}\t${summary.max}")
    )
  }


  def computeClassesSizes(df: DataFrame): Seq[(Int, Long)] = {
    if (df.hasColumn("y")) {
      val r = df.select("y")
        .groupBy("y")
        .agg(count("y"))
        .collect()
        .map(r => (r.getInt(0), r.getLong(1)))
        .sortBy(_._2)
        .reverse

      val sumClassesSize = r.map(_._2).sum
      if (sumClassesSize != df.count) {
        logger.warn(s"Sumclasses size $sumClassesSize differenet of data size")
      }

      r
    }
    else Seq[(Int, Long)]()
  }

  def computeFieldsSummary(df: DataFrame, dim: Integer, fieldNames: Array[String]): Seq[FieldSummary] = {
    val summary = df.summary("min", "max").collect
    (1 to dim).map(col =>
      FieldSummary(
        fieldNames(col - 1),
        removeDotZero(summary(0).getString(col)),
        removeDotZero(summary(1).getString(col)))
    )
  }

  def removeDotZero(s: String): String = s match {
    case dotCeroRegex(number) => number
    case _ => s
  }

  def computeFieldType(summary: FieldSummary): (FieldSummary, Char) = {
    summary match {
      case FieldSummary(_, "0", "1") =>
        (summary, 'B')
      case FieldSummary(_, min, max) if isInteger(min) && isInteger(max) =>
        (summary, 'I')
      case FieldSummary(_, min, max) if isReal(min) || isReal(max) =>
        (summary, 'R')
      case _ =>
        (summary, 'O')
    }
  }

  def isInteger(s: String): Boolean = s match {
    case integerRegex(_) => true
    case _ => false
  }

  def isReal(s: String): Boolean = s match {
    case realRegex(_) => true
    case _ => false
  }

}
