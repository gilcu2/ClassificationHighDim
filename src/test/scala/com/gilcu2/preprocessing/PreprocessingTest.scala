package com.gilcu2.preprocessing

import com.gilcu2.datasets.DatasetExtension._
import com.gilcu2.interfaces.Spark.loadCSVFromLines
import com.gilcu2.preprocessing.Preprocessing.makeApplyAfterVectorPipeline
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, VectorIndexer}
import org.apache.spark.ml.{Pipeline, linalg}
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}
import testUtil.SparkSessionTestWrapper
import testUtil.UtilTest._
import org.apache.spark.sql.functions._

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
    val withVectors = data.toFeatureVector()

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
    val withVectors = data.toFeatureVector()

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

  it should "preprocess hot encode categorial features features" in {

    Given("the data with categorical features features")
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
    val withVectors = data.toFeatureVector(removeAllColumns = false, outputColumn = "features0")
    withVectors.show()

    val indexer = new VectorIndexer()
      .setInputCol("features0")
      .setOutputCol("indexed0")
      .setMaxCategories(4)

    val indexerModel0 = indexer.fit(withVectors)

    println(s"Map ${indexerModel0.categoryMaps} ")

    val categoricalColumns = indexerModel0.categoryMaps
      .filter(_._2.size > 2).map(pair => data.columns(pair._1)).toArray

    val withVectorForCategoricalColumns = data
      .toFeatureVector(removeAllColumns = false, categoricalColumns, outputColumn = "FeaturesCategorical")

    val indexer1 = new VectorIndexer()
      .setInputCol("FeaturesCategorical")
      .setOutputCol("indexedCategorical")
      .setMaxCategories(4)

    val indexerModel1 = indexer1.fit(withVectorForCategoricalColumns)

    println(s"Map ${indexerModel0.categoryMaps} ")

    val withCategoricalFieldIndexed = indexerModel1.transform(withVectorForCategoricalColumns)
    withCategoricalFieldIndexed.show()

    val vecToArray = udf((xs: linalg.Vector) => xs.toArray)
    val withCategoricalColumnsAsArray = withCategoricalFieldIndexed
      .withColumn("indexedCategoricalAsArray", vecToArray($"indexedCategorical"))

    val sqlExpr = categoricalColumns
      .zipWithIndex.map { case (alias, idx) =>
      col("indexedCategoricalAsArray").getItem(idx).as(alias + "Indexed")
    }

    val withCategoricalIndexedAsColumns = withCategoricalColumnsAsArray.select(sqlExpr: _*)

    When("the hot encoder is fitted and the data transformed")

    val indexedColumns = categoricalColumns.map(_ + "Indexed")
    val encodedColumns = categoricalColumns.map(_ + "Encoded")
    val encoder = new OneHotEncoderEstimator()
      .setInputCols(indexedColumns)
      .setOutputCols(encodedColumns)

    val model = encoder.fit(withCategoricalIndexedAsColumns)
    val withEncoded = model.transform(withCategoricalIndexedAsColumns)

    Then("the df must have the new encoded columns")
    withEncoded.show
    withEncoded.columns.toSet.contains("DEncoded") shouldBe true

  }


}
