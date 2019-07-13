package com.gilcu2.sparkcollection

import com.gilcu2.interfaces.HadoopFS.delete
import org.apache.spark.sql.Dataset

sealed case class Format(code: String)

object Csv extends Format("csv")

object Json extends Format("json")

object Svm extends Format("libsvm")

object DatasetExtension {

  implicit class ExtendedDataset[T](ds: Dataset[T]) {

    def save(path: String, format: Format): Unit = {

      val pathWithExt = s"$path.${format.code}"

      delete(pathWithExt)

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

  }


}
