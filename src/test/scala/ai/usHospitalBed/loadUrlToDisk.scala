package ai.usHospitalBed

import org.scalatest.FunSpec
import org.scalatest.TryValues._
import org.apache.spark.sql.DataFrame
import org.scalatest.Assertions._
import scala.util.{Failure, Success}

class loadUrlToDiskTest
  extends FunSpec
  with SparkSessionTestWrapper {
    
  import spark.implicits._

  describe("urlLoader") {

    it("returns failure with empty url") {
      val result = loadUrlToDisk("", spark)
      assert(result.failure.exception.isInstanceOf[java.net.MalformedURLException])
    }

    it("returns a tmp file location") {
      val tmpFile = loadUrlToDisk.tmpFile
      assert(tmpFile.getParent().contains(System.getProperty("java.io.tmpdir")))
    }

    it("returns DataFrame after download geojson from the correct url") {
      val result = loadUrlToDisk.load("https://opendata.arcgis.com/datasets/1044bb19da8d4dbfb6a96eb1b4ebf629_0.geojson", spark)
      assert(result.schema == loadUrlToDisk.schema)
    }

    it("returns parquet file path after save the cleaned data") {
      val path = loadUrlToDisk("https://opendata.arcgis.com/datasets/1044bb19da8d4dbfb6a96eb1b4ebf629_0.geojson", spark)
      assert(path.success.value == loadUrlToDisk.ParquetDirectory)
    }
  }
  
}