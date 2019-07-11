package com.gilcu2.exploration

import org.apache.spark.sql.DataFrame

object Exploration {

  def summarize(df: DataFrame): DataFrame =
    df.summary()

}
