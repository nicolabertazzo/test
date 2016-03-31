package it.eng.exabeat.startkit

import it.eng.exabeat.startkit.model.Person
import it.eng.exabeat.test.commons.{LocalSqlSparkContext, ResourceAccessor, TempDirSupport}
import org.scalatest.{FlatSpec, Matchers}

/**
  * @author Antonio Murgia 
  * @version 25/03/16
  */
class SparkSqlDSxmlTestSuite extends FlatSpec with Matchers with ResourceAccessor with LocalSqlSparkContext with TempDirSupport {

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
    personList.toDS.toDF().write
      .format("com.databricks.spark.xml")
      .option("rootTag", "people")
      .option("rowTag", "person")
      .save(outputFile)
    val persons = sqlContext.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "person")
      .load(outputFile).as[Person]
    persons.collect() should contain theSameElementsAs (personList)
  }

  it should "read an xml Person file" in {
    import sqlContext.implicits._
    sqlContext.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "person")
      .load(getRelativeResourcePath()).as[Person]
      .collect() should contain theSameElementsAs (personList)
  }

}
