package it.eng.exabeat.test.commons

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, Suite}

/**
  * This is a commodity trait to be mixed to scalatest suites in order to have a freshly instantiated
  * SparkContext during the test run accessible using the [sparkContext] method.
  *
  * @author Antonio Murgia
  * @version 25/03/16
  */
trait LocalSparkContext extends Suite with BeforeAndAfterAll {
  this: Suite =>

  private var _sc: SparkContext = _

  def sparkContext: SparkContext = _sc

  protected val sparkConf = new SparkConf().setMaster("local").setAppName(getClass.getSimpleName)

  override def beforeAll(): Unit = {
    import org.apache.log4j.Logger
    import org.apache.log4j.Level
//    Logger.getLogger("org").setLevel(Level.OFF)
//    Logger.getLogger("akka").setLevel(Level.OFF)

    System.clearProperty("spark.driver.property")
    System.clearProperty("spark.hostPort")
    _sc = new SparkContext(sparkConf)
  }

  override def afterAll(): Unit = {
    _sc.stop()
    _sc = null
    System.clearProperty("spark.driver.property")
    System.clearProperty("spark.hostPort")
  }
}
