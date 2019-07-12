package com.gilcu2.interfaces

import org.apache.spark.sql._
import HadoopFS._
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, sum, when}

object Spark {

  def sparkSession(sparkConf: SparkConf): SparkSession =
    SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

  def readTextFile(path: String)(implicit spark: SparkSession): Dataset[String] =
    spark.read.textFile(path)


  def loadCSVFromLines(lines: Dataset[String], delimiter: String = ",", header: Boolean = true)(implicit sparkSession: SparkSession): DataFrame = {

    sparkSession.read
      .option("header", header)
      .option("delimiter", delimiter)
      .option("inferSchema", "true")
      .csv(lines)
  }

  def loadCSVFromFile(path: String, delimiter: String = ",", header: Boolean = true)(implicit sparkSession: SparkSession): DataFrame = {
    val lines = readTextFile(path)
    loadCSVFromLines(lines)
  }

  def writeCsv(df: DataFrame,
               path: String,
               header: Boolean = true,
               delimiter: String = ",",
               overrideFile: Boolean = true)(implicit sparkSession: SparkSession): Unit = {

    delete(path)

    df.write
      .option("header", header)
      .option("delimiter", delimiter)
      .csv(path)

  }

  implicit class ExtendedDataFrame(df: DataFrame) {

    def existColumn(name: String) = df.columns.exists(_ == name)

    def countNullsPerColumn: Seq[(String, Long)] = {
      val allColumns = df.columns
      val rowResults = df
        .select(allColumns.map(c => (sum(when(col(c).isNull, 1))).alias(c)): _*)
        .head()

      (0 to allColumns.size - 1).map(col =>
        if (rowResults.isNullAt(col))
          (allColumns(col), 0L)
        else
          (allColumns(col), rowResults.getLong(col))
      )
        .sortBy(_._2)
        .reverse
    }

    def rmColumnsWithNull: DataFrame = {
      val columnsWithoutNull = df.countNullsPerColumn.filter(_._2 == 0).map(_._1).toArray
      df.select(columnsWithoutNull.head, columnsWithoutNull.tail: _*)

    }

  }

}