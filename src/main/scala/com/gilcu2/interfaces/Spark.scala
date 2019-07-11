package com.gilcu2.interfaces

import org.apache.spark.sql._
import HadoopFS._
import org.apache.spark.SparkConf

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

}