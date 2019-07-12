package com.gilcu2.interfaces

import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}
import testUtil.SparkSessionTestWrapper
import testUtil.UtilTest._
import Spark._

class SparkTest extends FlatSpec with Matchers with GivenWhenThen with SparkSessionTestWrapper {

  behavior of "Spark"

  implicit val sparkSession = spark

  import spark.implicits._

  it should "load the data from csv lines" in {

    Given("the csv lines")
    val lines = readTextFile("data/sample.csv")

    When("the csv is loaded")
    val data = loadCSVFromLines(lines)

    Then("it must have the expected number of rows and columns")
    data.count shouldBe 100L
    data.columns.size shouldBe 305

  }

  it should "remove the columns with nulls" in {

    Given("the data")
    val lines =
      """
        |A,B,C
        |,1,2
        |3,,4
        |4,,6
        |1,2,3
      """.cleanLines
    val data = loadCSVFromLines(spark.createDataset(lines))

    When("null columns are removed")
    val withoutNullColumns = data.rmColumnsWithNull

    Then("the columns with null must be A,B")
    withoutNullColumns.columns shouldBe Array("C")

  }

  it should "count the number of nulls per column" in {

    Given("the data")
    val lines =
      """
        |A,B,C
        |,1,2
        |3,,4
        |4,,6
        |1,2,3
      """.cleanLines
    val data = loadCSVFromLines(spark.createDataset(lines))

    When("the counting is done")
    val columnsNullCount = data.countNullsPerColumn

    Then("the columns with null must be A,B")
    columnsNullCount.toSet shouldBe Set(("A", 1L), ("B", 2L), ("C", 0L))

  }

}
