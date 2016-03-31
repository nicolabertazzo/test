package it.eng.exabeat.startkit

import it.eng.exabeat.startkit.model.Person
import it.eng.exabeat.test.commons.{LocalSqlSparkContext, ResourceAccessor, TempDirSupport}
import org.scalatest.{FlatSpec, Matchers}

/**
  * @author Antonio Murgia 
  * @version 25/03/16
  */
class SparkSqlDFxmlTestSuite extends FlatSpec with Matchers with ResourceAccessor with LocalSqlSparkContext with TempDirSupport {

  lazy val personList = List(
    Person("Giuseppe", "Garibaldi", 99),
    Person("Nanni", "Moretti", 32),
    Person("Quentin", "Tarantino", 88),
    Person("Antonio", "Murgia", 12),
    Person("Mickey", "Mouse", 19),
    Person("Giovanni", "Neri", 105))

  it should "write an xml Person file" in {
    import sqlContext.implicits._
    val outputFile = createTempDir().getAbsolutePath + "output"
    personList.toDF().write
      .format("com.databricks.spark.xml")
      .option("rootTag", "people")
      .option("rowTag", "person")
      .save(outputFile)
    val persons = sqlContext.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "person")
      .load(outputFile)
      .map { row =>
        Person(row.getAs[String]("name"), row.getAs[String]("surname"), row.getAs[Long]("age"))
      }
    persons.collect() should contain theSameElementsAs (personList)
  }

  it should "read an xml Person file" in {
    sqlContext.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "person")
      .load(getRelativeResourcePath())
      .map { row =>
        Person(row.getAs[String]("name"), row.getAs[String]("surname"), row.getAs[Long]("age"))
      }.collect() should contain theSameElementsAs (personList)
  }

}
