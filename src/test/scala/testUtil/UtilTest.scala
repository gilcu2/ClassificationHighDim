package testUtil

import org.scalatest.{FlatSpec, Matchers}

object UtilTest extends FlatSpec with Matchers {

  implicit class ExtendedString(s: String) {

    def computeCleanLines: Array[String] = s.stripMargin.split("\n").map(_.trim).filter(_.nonEmpty)

  }

}
