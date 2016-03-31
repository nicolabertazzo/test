package it.eng.exabeat.startkit

import it.eng.exabeat.test.commons.{LocalSparkContext, ResourceAccessor}
import org.scalatest.{FlatSpec, Matchers}

/**
  * @author Antonio Murgia 
  * @version 25/03/16
  */
class SparkExampleTestSuite extends FlatSpec with Matchers with LocalSparkContext with ResourceAccessor {
  it should "open a test resource file and iterate through it elements" in {
    val dummyFile = sparkContext.textFile(getRelativeResourcePath())
    dummyFile.count() should be(7)
  }
}
