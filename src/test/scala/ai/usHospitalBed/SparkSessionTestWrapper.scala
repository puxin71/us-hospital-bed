package ai.usHospitalBed

import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {
  private lazy val cores = Runtime.getRuntime().availableProcessors()

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master(s"local[$cores]")
      .appName("spark session")
      .getOrCreate()
  }

}
