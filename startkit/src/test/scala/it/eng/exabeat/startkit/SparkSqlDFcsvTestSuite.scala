package it.eng.exabeat.startkit

import it.eng.exabeat.startkit.model.Person
import it.eng.exabeat.test.commons.{LocalSqlSparkContext, ResourceAccessor, TempDirSupport}
import org.scalatest.{FlatSpec, Matchers}

/**
  * CSV files fields read without inferring schema should be accessed using the position (like an array)
  * @author Antonio Murgia 
  * @version 25/03/16
  */
class SparkSqlDFcsvTestSuite extends FlatSpec with Matchers with ResourceAccessor with LocalSqlSparkContext with TempDirSupport {
  lazy val personList = List(
    Person("Giuseppe", "Garibaldi", 99),
    Person("Nanni", "Moretti", 32),
    Person("Quentin", "Tarantino", 88),
    Person("Antonio", "Murgia", 12),
    Person("Mickey", "Mouse", 19),
    Person("Giovanni", "Neri", 105))

  it should "write a csv Person file" in {
    import sqlContext.implicits._
    val outputFile = createTempDir().getAbsolutePath + "output"
    personList.toDF.write.format("com.databricks.spark.csv").save(outputFile)
    val persons = sqlContext.read
      .format("com.databricks.spark.csv")
      .load(outputFile)
      .map { row =>
        Person(row.getAs[String](0), row.getAs[String](1), row.getAs[String](2).toLong)
      }
    persons.collect() should contain theSameElementsAs (personList)
  }


  it should "read a csv Person file" in {
    val persons = sqlContext.read
      .format("com.databricks.spark.csv")
      .load(getRelativeResourcePath())
      .map { row =>
        Person(row.getAs[String](0), row.getAs[String](1), row.getAs[String](2).toLong)
      }
    persons.collect() should contain theSameElementsAs (personList)
  }
}
