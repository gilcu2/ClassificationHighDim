package com.gilcu2.interfaces

import com.gilcu2.interfaces.Spark.loadCSVFromLines
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}
import testUtil.SparkSessionTestWrapper
import DataFrame._
import testUtil.UtilTest._

class DataFrameTest extends FlatSpec with Matchers with GivenWhenThen with SparkSessionTestWrapper {

  behavior of "DataFrame"
  implicit val spaekSession = spark

  import spark.implicits._

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

  it should "count the total number of zeros" in {

    Given("the data")
    val lines =
      """
        |A,B,C
        |0,1,2
        |3,0.0,4
        |4,0,6
        |1,0.0,0
      """.cleanLines
    val data = loadCSVFromLines(spark.createDataset(lines))

    When("the counting is done")
    val cerosCount = data.countNumberOfZeros

    Then("the columns with null must be A,B")
    cerosCount shouldBe 5L

  }

  it should "convect to feature vector" in {

    Given("the data")
    val lines =
      """
        |A,B,C,y
        |0,1,2,1
        |3,0.0,4,2
        |4,0,6,1,
        |1,0.0,0,2
      """.cleanLines
    val data = loadCSVFromLines(spark.createDataset(lines))

    When("the data is transformed")
    val withFeatureVector = data.toFeatureVector

    Then("the columns with null must be A,B")
    withFeatureVector.columns shouldBe Array("y", "features")

  }

  it should "convect to labeled point" in {

    Given("the data")
    val lines =
      """
        |A,B,C,y
        |0,1,2,1
        |3,0.0,4,2
        |4,0,6,1,
        |1,0.0,0,2
      """.cleanLines
    val data = loadCSVFromLines(spark.createDataset(lines))
    val withFeatureVector = data.toFeatureVector

    When("the data is transformed")
    val withLabeledPoint =

      Then("the columns with null must be A,B")
    withFeatureVector.columns shouldBe Array("y", "features")

  }


}