package com.gilcu2.interfaces

import org.apache.spark.SparkConf
import org.apache.spark.sql._

object Spark {

  def sparkSession(sparkConf: SparkConf): SparkSession =
    SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

  def loadCSVFromFile(path: String, delimiter: String = ",", header: Boolean = true)(implicit sparkSession: SparkSession): DataFrame = {
    val lines = readTextFile(path + ".csv")
    loadCSVFromLines(lines)
  }

  def readTextFile(path: String)(implicit spark: SparkSession): Dataset[String] =
    spark.read.textFile(path)

  def loadCSVFromLines(lines: Dataset[String], delimiter: String = ",", header: Boolean = true)(implicit sparkSession: SparkSession): DataFrame = {

    sparkSession.read
      .option("header", header)
      .option("delimiter", delimiter)
      .option("inferSchema", "true")
      .csv(lines)
  }

  def getTotalCores(implicit spark: SparkSession): Int = {
    val executors = spark.sparkContext.statusTracker.getExecutorInfos
    val nExecutors = executors.size
    val nCores = spark.sparkContext.defaultParallelism
    nExecutors * nCores
  }

}