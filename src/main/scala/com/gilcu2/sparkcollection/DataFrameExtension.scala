package com.gilcu2.sparkcollection

import com.gilcu2.interfaces.Time
import org.apache.spark.ml.feature.{LabeledPoint, MinMaxScaler, Normalizer, VectorAssembler}
import org.apache.spark.ml.linalg
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, _}

case class LearnMatrix(features: DataFrame, labels: DataFrame)

object DataFrameExtension {

  implicit class ExtendedDataFrame(df: DataFrame) {

    val CLASS_FIELD = "y"
    val FEATURES_FIELD = "features"

    def rmColumnsWithNull: DataFrame = {
      val (columnsWithoutNullCount, columnsWithNullCount) = df.countNullsPerColumn.partition(_._2 == 0)
      val columnsWithNull = columnsWithNullCount.map(_._1)
      val columnsWithoutNull = columnsWithoutNullCount.map(_._1)

      println(s"Removed columns because have null: $columnsWithNull")

      df.select(columnsWithoutNull.head, columnsWithoutNull.tail: _*)
    }

    def countNullsPerColumn: Seq[(String, Long)] = {
      val allColumns = df.columns
      val rowResults = df
        .select(allColumns.map(c => sum(when(col(c).isNull, 1)).alias(c)): _*)
        .head()

      allColumns.indices.map(col =>
        if (rowResults.isNullAt(col))
          (allColumns(col), 0L)
        else
          (allColumns(col), rowResults.getLong(col))
      )
        .sortBy(_._2)
        .reverse
    }

    def countRowsWithNullOrEmptyString: Long = {
      val cond = df.columns.map(x => col(x).isNull || col(x) === "").reduce(_ || _)
      df.filter(cond).count
    }

    def countNumberOfZeros: Long = {
      val countZeros = df.columns.map(x => when(col(x) === 0, 1).otherwise(0))
        .reduce(_ + _)
      df.withColumn("nullCount", countZeros)
        .agg(sum("nullCount").cast("long"))
        .first.getLong(0)
    }

    def toFeatureVector: DataFrame = {

      println(s"toFeatureVector ${Time.getCurrentTime}")

      val columns = df.columns.toSet
      val hasClassColumn = columns.contains("y")
      val featureColumns = (columns - "y").toArray
      val assembler = new VectorAssembler()
        .setInputCols(featureColumns)
        .setOutputCol(FEATURES_FIELD)

      val withFeatures = assembler.transform(df)
      if (hasClassColumn)
        withFeatures.select(CLASS_FIELD, FEATURES_FIELD)
      else
        withFeatures.select(FEATURES_FIELD)
    }

    def toLabeledPoints(implicit spark: SparkSession): Dataset[LabeledPoint] = {

      println(s"toLabeledPoints ${Time.getCurrentTime}")

      import spark.implicits._
      df.map(row => LabeledPoint(row.getAs[Int](CLASS_FIELD).toDouble,
        row.getAs[linalg.Vector](FEATURES_FIELD)))
    }

    def scaleFeatures(implicit spark: SparkSession): DataFrame = {

      println(s"scaleFeatures ${Time.getCurrentTime}")

      val tempField = "scaledFeatures"

      val scaler = new MinMaxScaler()
        .setInputCol(FEATURES_FIELD)
        .setOutputCol(tempField)

      val scalerModel = scaler.fit(df)
      val withScaledFeatures = scalerModel.transform(df)
      val renamedFields = withScaledFeatures
        .withColumnRenamed(FEATURES_FIELD, "oldFeatures")
        .withColumnRenamed(tempField, FEATURES_FIELD)
      renamedFields.select(df.columns.head, df.columns.tail: _*)
    }


  }

}
