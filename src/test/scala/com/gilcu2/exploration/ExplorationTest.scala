package com.gilcu2.exploration

import com.gilcu2.interfaces.Spark._
import org.scalatest._
import testUtil.SparkSessionTestWrapper


class ExplorationTest extends FlatSpec with Matchers with GivenWhenThen with SparkSessionTestWrapper {

  behavior of "Spark"

  implicit val sparkSession = spark

  it should "summarize data" in {

    Given("the data")
    val data = loadCSVFromFile("data/sample.csv")

    When("the is described")
    val summary = Exploration.summarizeFields(data)

    Then(" the description must have the number of fields expected")
    summary.size shouldBe 305

  }

}
