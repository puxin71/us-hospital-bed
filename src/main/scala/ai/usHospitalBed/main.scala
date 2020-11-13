package ai.usHospitalBed

import scala.util.{Try, Success, Failure}
import org.apache.spark.sql.DataFrame

object main extends SparkSessionWrapper {
  def main(args: Array[String]) {
    if (args.length <= 0) {
      println("usage: main url")
      System.exit(1)
    }

    urlLoader(args(0), spark) match {
      case Failure(e) => println(e)
      case Success(r) => println(r)
    }
  }
  
}
