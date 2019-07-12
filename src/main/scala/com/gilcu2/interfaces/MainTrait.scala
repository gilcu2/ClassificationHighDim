package com.gilcu2.interfaces

import com.gilcu2.interfaces.Time.{getCurrentTime, getHumanDuration}
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait ConfigValuesTrait

trait LineArgumentValuesTrait

trait MainTrait extends LazyLogging {

  def getConfigValues(conf: Config): ConfigValuesTrait

  def getLineArgumentsValues(args: Array[String], configValues: ConfigValuesTrait): LineArgumentValuesTrait

  def process(configValues: ConfigValuesTrait, lineArguments: LineArgumentValuesTrait)(
    implicit spark: SparkSession
  ): Unit

  def main(implicit args: Array[String]): Unit = {

    val beginTime = getCurrentTime
    logger.info(s"Begin: $beginTime")
    logger.info(s"Arguments: $args")

    println(s"Begin: $beginTime")

    implicit val conf = ConfigFactory.load
    val sparkConf = new SparkConf().setAppName("Exploration")
    implicit val spark = Spark.sparkSession(sparkConf)

    val configValues = getConfigValues(conf)
    val lineArguments = getLineArgumentsValues(args, configValues)

    process(configValues, lineArguments)

    val endTime = getCurrentTime
    val humanTime = getHumanDuration(beginTime, endTime)
    logger.info(s"End: $endTime Total: $humanTime")
    println(s"End: $endTime Total: $humanTime")

  }

}
