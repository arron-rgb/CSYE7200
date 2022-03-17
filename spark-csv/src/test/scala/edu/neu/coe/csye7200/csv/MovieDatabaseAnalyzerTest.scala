package edu.neu.coe.csye7200.csv

import org.apache.spark.sql.Dataset
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.{Success, Try}

class MovieDatabaseAnalyzerTest extends AnyFlatSpec with Matchers {

  behavior of "parseResource"
  it should "get movie_metadata.csv" in {

    val analyzer = MovieDatabaseAnalyzer("/movie_metadata.csv")
    val mdy: Try[Dataset[Movie]] = analyzer.dy
    mdy.isSuccess shouldBe true
    mdy foreach {
      d =>
        d.count() shouldBe 1567
        d.show(10)
    }

    val row = analyzer.average(mdy, "reviews.imdbScore")
    val r = row match {
      case Success(v) => v.getDouble(0)
      case _ => 0
    }
    r - 6.435 < 0.001 shouldBe true

    val std = analyzer.std(mdy, "reviews.imdbScore")
    val s = std match {
      case Success(v) => v.getDouble(0)
      case _ => 0
    }
    s - 0.9925 < 0.001 shouldBe true

  }


}
