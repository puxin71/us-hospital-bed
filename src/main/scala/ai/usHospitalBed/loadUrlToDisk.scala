package ai.usHospitalBed

import scala.io.Source._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import scala.util.Try
import org.apache.spark.sql.SparkSession
import java.net.URL
import java.io.File
import java.time.Instant
import sys.process._

object loadUrlToDisk {
  private val Delimiter = "_"
  val ParquetDirectory = "/tmp/usHospitalBeds"
  
  /**
    * LoadUrlToDisk downloads the raw dataset from a URL, extract the expected data and remove null values, and save the cleaned data to parquets
    *
    * @param url
    * @param spark
    * @return the directory of the parquet files
    */
  def apply(url: String, spark: SparkSession): Try[String] = Try{  
    val excludedNulls = 
      load(url, spark)
        .na
        .drop("all", Seq("properties"))
        .select("properties.*")

    save(excludedNulls, spark)        
  }

  def tmpFile: File = new File(System.getProperty("java.io.tmpdir"), "bedCounts" + Delimiter + Instant.now.getEpochSecond.toString())

  def schema: StructType = {
    val geometry = new StructType()
      .add("coordinates", ArrayType(DoubleType))
      .add("type", StringType)

    val properties = new StructType()
      .add("ADULT_ICU_BEDS", LongType)
      .add("AVG_VENTILATOR_USAGE", DoubleType)
      .add("BED_UTILIZATION", DoubleType)
      .add("CNTY_FIPS", StringType)
      .add("COUNTY_NAME", StringType)
      .add("HOSPITAL_NAME", StringType)
      .add("HOSPITAL_TYPE", StringType)
      .add("HQ_ADDRESS", StringType)
      .add("HQ_ADDRESS1", StringType)
      .add("HQ_CITY", StringType)
      .add("HQ_STATE", StringType)
      .add("HQ_ZIP_CODE", StringType)
      .add("NUM_ICU_BEDS", LongType)
      .add("NUM_LICENSED_BEDS", LongType)
      .add("OBJECTID", LongType)
      .add("PEDI_ICU_BEDS", DoubleType)
      .add("Potential_Increase_In_Bed_Capac", LongType)
      .add("STATE_NAME", StringType)

    new StructType()
      .add("_corrupt_record",StringType)
      .add("geometry", geometry)
      .add("properties", properties)
      .add("type",StringType)    
  }

  def load(url: String, spark: SparkSession): DataFrame = {
    val localFile = tmpFile
    fileDownloader(url, localFile.getAbsolutePath())

    val jsonDf = 
      spark
        .read
        .schema(schema)
        .json(localFile.getAbsolutePath())

    localFile.deleteOnExit()
    jsonDf
  }

  private def fileDownloader(url: String, filename: String) = {
    new URL(url) #> new File(filename) !!
  }

  private def save(df: DataFrame, spark: SparkSession): String = {
    val parquetPath = 
    df
      .write
      .mode("overwrite")
      .format("parquet")
      .save(ParquetDirectory)
    ParquetDirectory
  }
}