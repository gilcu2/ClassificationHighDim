package com.gilcu2.datasets

import com.gilcu2.interfaces.HadoopFS
import com.gilcu2.interfaces.Time
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object DatasetExtension {

  val CLASS_FIELD = "y"
  val FEATURES_FIELD = "features"

  implicit class ExtendedDataset[T](ds: Dataset[T]) {

    def save(path: String, format: FileFormat): Unit = {

      val pathWithExt = s"$path.${format.code}"

      HadoopFS.delete(pathWithExt)

      ds.write.format(format.code).save(pathWithExt)
      println(s"$pathWithExt saved")
    }

    def hasColumn(name: String): Boolean = ds.columns.contains(name)

    def smartShow(label: String): Unit = {
      println(s"\nFirst rows with some columns of $label")
      val columnsToShow = ds.columns.take(20)
      val dataToShow = if (ds.hasColumn("y"))
        ds.select("y", columnsToShow: _*)
      else
        ds.select(columnsToShow.head, columnsToShow.tail: _*)

      dataToShow.show(10, 10)
    }

    def transform[TI, TO](source: Dataset[TI], f: Dataset[TI] => Dataset[TO], label: String): Dataset[TO] = {
      val ds = f(source)
      ds.persist()

      ds.smartShow(label)
      source.unpersist()
      ds
    }

    def countNullsPerColumn: Seq[(String, Long)] = {
      val allColumns = ds.columns
      val rowResults = ds
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
      val cond = ds.columns.map(x => col(x).isNull || col(x) === "").reduce(_ || _)
      ds.filter(cond).count
    }

    def countNumberOfZeros: Long = {
      val countZeros = ds.columns.map(x => when(col(x) === 0, 1).otherwise(0))
        .reduce(_ + _)
      ds.withColumn("nullCount", countZeros)
        .agg(sum("nullCount").cast("long"))
        .first.getLong(0)
    }

    def rmColumnsWithNull: DataFrame = {
      val (columnsWithoutNullCount, columnsWithNullCount) = ds.countNullsPerColumn.partition(_._2 == 0)
      val columnsWithNull = columnsWithNullCount.map(_._1)
      val columnsWithoutNull = columnsWithoutNullCount.map(_._1)

      println(s"Removed columns because have null: $columnsWithNull")

      ds.select(columnsWithoutNull.head, columnsWithoutNull.tail: _*)
    }

    def toFeatureVector: DataFrame = {

      println(s"toFeatureVector ${Time.getCurrentTime}")

      val (columns, featureColumns) = getFeaturedColumns
      val hasClassColumn = columns.contains(CLASS_FIELD)
      val assembler = new VectorAssembler()
        .setInputCols(featureColumns)
        .setOutputCol(FEATURES_FIELD)

      val withFeatures = assembler.transform(ds)
      if (hasClassColumn)
        withFeatures.select(CLASS_FIELD, FEATURES_FIELD)
      else
        withFeatures.select(FEATURES_FIELD)
    }

    def getFeaturedColumns: (Set[String], Array[String]) = {
      val columns = ds.columns.toSet
      val featureColumns = (columns - CLASS_FIELD).toArray
      (columns, featureColumns)
    }

    def scaleFeatures(implicit spark: SparkSession): DataFrame = {

      println(s"scaleFeatures ${Time.getCurrentTime}")

      val tempField = "scaledFeatures"

      val scaler = new MinMaxScaler()
        .setInputCol(FEATURES_FIELD)
        .setOutputCol(tempField)

      val scalerModel = scaler.fit(ds)
      val withScaledFeatures = scalerModel.transform(ds)
      val renamedFields = withScaledFeatures
        .withColumnRenamed(FEATURES_FIELD, "oldFeatures")
        .withColumnRenamed(tempField, FEATURES_FIELD)
      renamedFields.select(ds.columns.head, ds.columns.tail: _*)
    }

    def toLabeledPoints(implicit spark: SparkSession): Dataset[LabeledPoint] = {

      println(s"toLabeledPoints ${Time.getCurrentTime}")

      import spark.implicits._
      ds.map { case row: Row =>
        LabeledPoint(row.getAs[Int](CLASS_FIELD).toDouble, row.getAs[linalg.Vector](FEATURES_FIELD))
      }
    }



  }

}
