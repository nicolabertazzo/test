package it.eng.exabeat

import org.scalatest.{FlatSpec, Matchers}

/**
  * @author Antonio Murgia
  * @version 25/03/16
  */
class ScalaTestSuite extends FlatSpec with Matchers {
  it should "correctly compare strings" in {
    "hello" should be("hello")
    "bye" should be < ("hello")
  }
}
