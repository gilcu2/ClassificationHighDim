package com.gilcu2.preprocessing

import com.gilcu2.datasets.DatasetExtension._
import com.gilcu2.interfaces.Spark.loadCSVFromLines
import com.gilcu2.preprocessing.Preprocessing.makeApplyAfterVectorPipeline
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.{Pipeline, linalg}
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}
import testUtil.SparkSessionTestWrapper
import testUtil.UtilTest._

class PreprocessingTest extends FlatSpec with Matchers with GivenWhenThen with SparkSessionTestWrapper {

  behavior of "Preprocessing"

  implicit val spaekSession = spark

  import spark.implicits._

  it should "preprocess the features" in {

    Given("the data with features")
    val lines =
      """
        |A,B,C,D,y
        |0,1,2,3,1
        |5,0,4,0,2
        |2,0,6,2,3,
        |10,0,0,1,4
      """.cleanLines
    val data = loadCSVFromLines(spark.createDataset(lines))
    val withVectors = data.toFeatureVector

    When("preprocess")
    val (afterVector, _) =
      makeApplyAfterVectorPipeline(withVectors, scale = true, maxForCategories = 4)

    Then("features must be the expected")
    afterVector.head.getAs[linalg.Vector](FEATURES_FIELD)(2) shouldBe 2.0 / 6.0

  }

  it should "preprocess categorical features" in {

    Given("the data with features")
    val lines =
      """
        |A,B,C,D,y
        |0,1,2,4,1
        |5,0,4,0,2
        |2,0,6,5,3,
        |10,0,0,8,4
        |1,1,2,8,5
        |7,0,4,4,6
        |2,0,6,5,7
        |10,0,5,0,8
      """.cleanLines
    val data = loadCSVFromLines(spark.createDataset(lines))
    val withVectors = data.toFeatureVector

    When("preprocess")
    val indexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexed")
      .setMaxCategories(4)

    val indexerModel = indexer.fit(withVectors)

    println(s"Map ${indexerModel.categoryMaps} ")

    // Create new column "indexed" with categorical values transformed to indices
    val indexedData = indexerModel.transform(withVectors)
    indexedData.show()

    Then("features must be the expected")
    indexedData.head.getAs[linalg.Vector]("indexed")(3) shouldBe 1.0

  }


}
