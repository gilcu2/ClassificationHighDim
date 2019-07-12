package com.gilcu2.interfaces

import org.apache.spark.sql._
import org.apache.spark.SparkConf

object Spark {

  def sparkSession(sparkConf: SparkConf): SparkSession =
    SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

  def loadCSVFromFile(path: String, delimiter: String = ",", header: Boolean = true)(implicit sparkSession: SparkSession): DataFrame = {
    val lines = readTextFile(path)
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
    val workers = spark.sparkContext.statusTracker.getExecutorInfos.size
    val cores = spark.sparkContext.getConf.get("spark.executor.cores", "1").toInt
    workers * cores
  }

}