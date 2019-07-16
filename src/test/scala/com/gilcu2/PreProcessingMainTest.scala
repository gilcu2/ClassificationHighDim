package com.gilcu2

import com.gilcu2.datasets.DatasetExtension._
import com.gilcu2.interfaces.Spark.loadCSVFromLines
import org.apache.spark.ml.{Pipeline, linalg}
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}
import testUtil.SparkSessionTestWrapper
import testUtil.UtilTest._

class PreProcessingMainTest extends FlatSpec with Matchers with GivenWhenThen with SparkSessionTestWrapper {

  behavior of "PreProcessingMain"

  implicit val spaekSession = spark

  import spark.implicits._

  it should "scale the features" in {

    Given("the data with features")
    val lines =
      """
        |A,B,C,y
        |0,1,2,1
        |3,0,4,2
        |4,0,6,3,
        |1,0,0,4
      """.cleanLines
    val data = loadCSVFromLines(spark.createDataset(lines))
    val withFeatureVector = data.toFeatureVector

    And("the scaling pipeline")
    val (scaleStage, selectorStage) = PreProcessingMain.makeScalerStage(true, true)
    val pipeline = new Pipeline().setStages(Array(scaleStage.get, selectorStage.get))
    val model = pipeline.fit(withFeatureVector)

    When("normalize")
    val scaled = model.transform(withFeatureVector).collect()

    Then("features must be the expected")
    scaled.head.getAs[linalg.Vector]("features")(2) shouldBe 2.0 / 6.0


  }

}
