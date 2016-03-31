package it.eng.exabeat.test.commons

import org.apache.spark.sql.SQLContext
import org.scalatest.Suite

/**
  * @author Antonio Murgia 
  * @version 25/03/16
  */
trait LocalSqlSparkContext extends LocalSparkContext {
  this: Suite =>

  private var _sqlCtx: SQLContext = _

  lazy val sqlContext = _sqlCtx

  override def beforeAll() = {
    super.beforeAll()
    _sqlCtx = new SQLContext(sparkContext)
  }
}