package com.gilcu2.interfaces

import org.apache.spark.sql.Dataset

object Dataset {

  implicit class ExtendedDataset[T](ds: Dataset[T]) {

    def saveSVM(path: String): Unit = {
      ds.write.format("libsvm").save(path)
      println(s"svm $path saved")
    }

  }

}
