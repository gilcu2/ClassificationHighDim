package com.gilcu2.interfaces

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

object HadoopFS {

  val config = new Configuration()
  val fs = FileSystem.get(config)

  def exists(path: String): Boolean =
    path.nonEmpty && fs.exists(new Path(path))

  def delete(path: String): Boolean =
    fs.delete(new Path(path), true)

  def merge(srcPath: String, dstPath: String): Unit =
    FileUtil.copyMerge(fs, new Path(srcPath), fs, new Path(dstPath), false, config, null)

  def mkdir(path: String): Unit = fs.mkdirs(new Path(path))

  def getPath(url: String): String = new java.net.URI(url).getPath

  def ls(path: String) = {
    val status = fs.listStatus(new Path(path))
    status.map(_.getPath.getName)
  }

}