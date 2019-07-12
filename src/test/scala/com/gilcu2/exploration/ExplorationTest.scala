package com.gilcu2.exploration

import com.gilcu2.interfaces.Spark._
import org.apache.spark.sql.Row
import org.scalatest._
import testUtil.SparkSessionTestWrapper
import testUtil.UtilTest._

class ExplorationTest extends FlatSpec with Matchers with GivenWhenThen with SparkSessionTestWrapper {

  behavior of "Spark"

  implicit val sparkSession = spark

  import spark.implicits._

  it should "summarize data" in {

    Given("the data")
    val data = loadCSVFromFile("data/sample.csv")

    When("the is described")
    val summary = Exploration.summarizeFields(data)

    Then(" the description must have the number of fields expected")
    summary.size shouldBe 305

  }

  it should "compute classes sizes" in {

    Given("the data")
    val data = loadCSVFromFile("data/sample.csv")

    When("the is described")
    val classesSize = Exploration.computeClassesSizes(data)

    Then("the sum of the classes size must the number of objects")
    classesSize.map(_._2).sum shouldBe data.count

  }

  it should "count the number of rows with at least one null" in {

    Given("the data")
    val lines =
      """
        |A,B,C
        |,1,2
        |3,,
        |4,,6
        |1,2,3
      """.cleanLines
    val data = loadCSVFromLines(spark.createDataset(lines))

    When("the counting is done")
    val count = Exploration.countRowsWithNullOrEmptyString(data)

    Then("the columns with null must be A,B")
    count shouldBe 3L

  }

}
