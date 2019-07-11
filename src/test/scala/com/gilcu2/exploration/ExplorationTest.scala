package com.gilcu2.exploration

import com.gilcu2.interfaces.Spark._
import org.scalatest._
import testUtil.SparkSessionTestWrapper
import org.apache.spark.sql.functions._

class ExplorationTest extends FlatSpec with Matchers with GivenWhenThen with SparkSessionTestWrapper {

  behavior of "Spark"

  implicit val sparkSession = spark

  it should "summarize data" in {

    Given("the data")
    val data = loadCSVFromFile("data/sample.csv")

    When("the is described")
    val summary = Exploration.summarize(data)

    Then(" the description must be the expected")
    val x001 = summary.select("summary", "x001")
    val count = x001.filter("summary == count").collect
    count shouldBe 100

  }

}
