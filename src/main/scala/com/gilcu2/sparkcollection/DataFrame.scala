package com.gilcu2.sparkcollection

import org.apache.spark.ml.feature.{LabeledPoint, VectorAssembler}
import org.apache.spark.ml.linalg
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, _}

case class LearnMatrix(features: DataFrame, labels: DataFrame)

object DataFrameExtension {

  implicit class ExtendedDataFrame(df: DataFrame) {

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
      val columns = df.columns.toSet
      val hasClassColumn = columns.contains("y")
      val featureColumns = (columns - "y").toArray
      val assembler = new VectorAssembler()
        .setInputCols(featureColumns)
        .setOutputCol("features")

      val withFeatures = assembler.transform(df)
      if (hasClassColumn)
        withFeatures.select("y", "features")
      else
        withFeatures.select("features")
    }

    def toLabeledPoints(implicit spark: SparkSession): Dataset[LabeledPoint] = {
      import spark.implicits._
      df.map(row => LabeledPoint(row.getAs[Int]("y").toDouble,
        row.getAs[linalg.Vector]("features")))
    }


  }

}
