package ai.usHospitalBed

import scala.io.Source._
import org.apache.spark.sql.DataFrame
import scala.util.Try
import org.apache.spark.sql.SparkSession
import java.net.URL
import java.io.File
import java.time.Instant
import sys.process._

object urlLoader {
  def apply(url: String, spark: SparkSession): Try[DataFrame] = Try {
    val filename = tmpFile.getAbsolutePath()
    fileDownloader("https://opendata.arcgis.com/datasets/1044bb19da8d4dbfb6a96eb1b4ebf629_0.geojson", filename)

    val jsonDf = spark.read.json(filename)
    jsonDf 
  }

  def tmpFile: File = new File(System.getProperty("java.io.tmpdir"), "cindys" + "_" + Instant.now.getEpochSecond.toString())

  private def fileDownloader(url: String, filename: String) = {
    new URL(url) #> new File(filename) !!
  }
}