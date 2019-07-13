package com.gilcu2.datasets

sealed case class FileFormat(code: String)

object Csv extends FileFormat("csv")

object Json extends FileFormat("json")

object Svm extends FileFormat("libsvm")