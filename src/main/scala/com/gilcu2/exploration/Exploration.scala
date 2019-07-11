package com.gilcu2.exploration

import java.security.KeyStore.TrustedCertificateEntry

import org.apache.spark.sql.{DataFrame, Dataset}

import scala.collection.immutable

case class DataSummary(size: Long, dim: Int, fields: Seq[String],
                       booleanFields: Seq[String], integerFields: Seq[String],
                       realFields: Seq[String], otherFields: Seq[String],
                       fieldsSummary: Seq[FieldSummary])
case class FieldSummary(name: String, min: String, max: String)

object Exploration {
  val dotCeroRegex = "([0-9]+).0".r
  val integerRegex = "^(-?[0-9]+)$".r
  val realRegex = "^(-?[0-9]+.[0-9]+)$".r

  def summarizeFields(df: DataFrame): DataSummary = {

    val size = df.count
    val fieldNames = df.columns
    val dim = fieldNames.length

    val fieldsSummary = computeFieldsSummary(df, dim, fieldNames)
    val fieldTypes = fieldsSummary.map(summary => computeFieldType(summary))

    val booleanFields = fieldTypes.filter(_._2 == 'B').map(_._1.name)
    val integerFields = fieldTypes.filter(_._2 == 'I').map(_._1.name)
    val realFields = fieldTypes.filter(_._2 == 'R').map(_._1.name)
    val otherFields = fieldTypes.filter(_._2 == 'O').map(_._1.name)

    DataSummary(size, dim, fieldNames, booleanFields, integerFields, realFields, otherFields, fieldsSummary)

  }

  def printDataSummary(dataSummary: DataSummary, inputPath: String): Unit = {

    def printFieldTypes(fieldType: String, fields: Seq[String]): Unit =
      println(s"Fields $fieldType: ${fields.size}\n ${fields}\n")

    println("\nClassifier report\n")

    println(s"Input: $inputPath\n")

    println(s"Size: ${dataSummary.size}\n")
    println(s"Fields: ${dataSummary.fields.size}\n ${dataSummary.fields}\n")

    printFieldTypes("Boolean", dataSummary.booleanFields)
    printFieldTypes("Integer", dataSummary.integerFields)
    printFieldTypes("Real", dataSummary.realFields)
    printFieldTypes("Other", dataSummary.otherFields)

    println("Fields summary")
    println("Name\tMin\tMax")
    dataSummary.fieldsSummary.foreach(summary =>
      println(s"${summary.name}\t${summary.min}\t${summary.max}")
    )
  }


  def removeDotZero(s: String): String = s match {
    case dotCeroRegex(number) => number
    case _ => s
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

  def isInteger(s: String): Boolean = s match {
    case integerRegex(_) => true
    case _ => false
  }

  def isReal(s: String): Boolean = s match {
    case realRegex(_) => true
    case _ => false
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

}
