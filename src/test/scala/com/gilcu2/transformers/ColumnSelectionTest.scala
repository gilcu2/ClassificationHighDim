package com.gilcu2.transformers

import com.gilcu2.interfaces.Spark.loadCSVFromLines
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}
import testUtil.SparkSessionTestWrapper
import testUtil.UtilTest._

class ColumnSelectionTest extends FlatSpec with Matchers with GivenWhenThen with SparkSessionTestWrapper {

  behavior of "ColumnSelection"

  implicit val spaekSession = spark

  import spark.implicits._

  it should "select the columns" in {

    Given("A dataset")
    val lines =
      """
        |A,B,C,y
        |0,1,2,1
        |3,0.0,4,2
        |4,0,6,1,
        |1,0.0,0,2
      """.cleanLines
    val data = loadCSVFromLines(spark.createDataset(lines))

    And("the column selection transformer")
    val transformer = new ColumnSelector("test")
    transformer.setOutputColumns(Array("B", "y"))

    When("the transformer is applied")
    val result = transformer.transform(data)

    Then("the columns must be the expected")
    result.columns shouldBe Array("B", "y")

  }

}
