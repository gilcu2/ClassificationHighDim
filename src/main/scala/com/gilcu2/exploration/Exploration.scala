package com.gilcu2.exploration

import org.apache.spark.sql.{DataFrame, Dataset}

case class FieldSummary(name: String, min: String, max: String)

object Exploration {

  def summarizeFields(df: DataFrame): Seq[FieldSummary] = {
    val summary = df.summary("min", "max").collect
    val names = df.columns
    val numFields = names.length
    (1 to numFields).map(col =>
      FieldSummary(names(col - 1), summary(0).getString(col), summary(1).getString(col))
    )
  }

}
