package edu.neu.coe.csye7200.csv

import com.phasmidsoftware.table.Table
import org.apache.spark.sql.functions.{avg, stddev_pop}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.util.Try


/**
  * @author scalaprof
  */
case class MovieDatabaseAnalyzer(resource: String) {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("WordCount")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR") // We want to ignore all of the INFO and WARN messages.

  import MovieParser._
  import spark.implicits._

  private val mty: Try[Table[Movie]] = Table.parseResource(resource, getClass)
  val dy: Try[Dataset[Movie]] = mty map {
    mt =>
      println(s"Movie table has ${mt.size} rows")
      spark.createDataset(mt.rows.toSeq)
  }

  def average(df: Try[Dataset[Movie]], columnName: String): Try[Row] = for (d <- df) yield d.agg(avg(columnName)).collect()(0)

  def std(df: Try[Dataset[Movie]], columnName: String): Try[Row] = for (d <- df) yield d.agg(stddev_pop(columnName)).collect()(0)

}


/**
  * @author scalaprof
  */
object MovieDatabaseAnalyzer extends App {

  import Numeric.Implicits._

  def mean[T: Numeric](xs: Iterable[T]): Double = xs.sum.toDouble / xs.size

  def variance[T: Numeric](xs: Iterable[T]): Double = {
    val avg = mean(xs)
    xs.map(_.toDouble).map(a => math.pow(a - avg, 2)).sum / xs.size
  }

  def stdDev[T: Numeric](xs: Iterable[T]): Double = math.sqrt(variance(xs))

  val groupByTitle = (m: Movie) => m.title

  def apply(resource: String): MovieDatabaseAnalyzer = new MovieDatabaseAnalyzer(resource)


  apply("/movie_metadata.csv").dy foreach {
    d =>
      println(d.count())
      d.show(10)
  }
}

