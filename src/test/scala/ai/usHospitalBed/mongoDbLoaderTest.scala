package ai.usHospitalBed

import org.scalatest.FunSpec

class mongoDbLoaderTest 
  extends FunSpec
  with SparkSessionTestWrapper {

  import spark.implicits._

  describe("mongoDbLoader") {
    it("returns the expected dataset") {
      /*
      val eventsFromJSONDF = Seq (
        (0, """{"device_id": 0, "device_type": "sensor-ipad", "ip": "68.161.225.1", "cca3": "USA", "cn": "United States", "temp": 25, "signal": 23, "battery_level": 8, "c02_level": 917, "timestamp" :1475600496 }"""),
      val result = urlLoader("", spark)
      assert(result.failure.exception.isInstanceOf[java.net.MalformedURLException])
      */
    }    
  }
}
