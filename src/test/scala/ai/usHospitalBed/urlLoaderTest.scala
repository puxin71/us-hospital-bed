package ai.usHospitalBed

import org.scalatest.FunSpec
import org.scalatest.TryValues._
import org.apache.spark.sql.DataFrame

class urlLoaderTest
  extends FunSpec
  with SparkSessionTestWrapper {
    
  import spark.implicits._

  describe("urlLoader") {

    it("returns failure with empty url") {
      val result = urlLoader("", spark)
      assert(result.failure.exception.isInstanceOf[java.net.MalformedURLException])
    }

    it("returns sucess with the correct url") {
      val result = urlLoader("https://coronavirus-resources.esri.com/datasets/definitivehc::definitive-healthcare-usa-hospital-beds/data?geometry=92.988%2C-16.820%2C-117.950%2C72.123", spark)
      assert(result.success.value.isInstanceOf[DataFrame])
      println(result.success.value.columns.mkString)
      result.success.value.show(10)
    }
  }
}