package com.gilcu2.interfaces

import com.gilcu2.interfaces.Spark._
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}
import testUtil.SparkSessionTestWrapper

class SparkTest extends FlatSpec with Matchers with GivenWhenThen with SparkSessionTestWrapper {

  behavior of "Spark"

  implicit val sparkSession = spark

  it should "load the data from csv lines" in {

    Given("the csv lines")
    val lines = readTextFile("data/sample.csv")

    When("the csv is loaded")
    val data = loadCSVFromLines(lines)

    Then("it must have the expected number of rows and columns")
    data.count shouldBe 100L
    data.columns.length shouldBe 305

  }

}
