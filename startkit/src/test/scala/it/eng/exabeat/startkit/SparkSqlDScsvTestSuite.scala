package it.eng.exabeat.startkit

import it.eng.exabeat.startkit.model.Person
import it.eng.exabeat.test.commons.{LocalSqlSparkContext, ResourceAccessor, TempDirSupport}
import org.scalatest.{FlatSpec, Matchers}

/**
  * Using the DataSet API to read csv file the schema must be inferred (costly) or provided manually (verbose),
  * it is also mandatory to have the csv header describing the columns at the beginning of each file
  * @author Antonio Murgia 
  * @version 25/03/16
  */
class SparkSqlDScsvTestSuite extends FlatSpec with Matchers with ResourceAccessor with LocalSqlSparkContext with TempDirSupport {
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
    personList.toDS.toDF.write.format("com.databricks.spark.csv").option("header", "true").save(outputFile)
    val persons = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true") // Automatically infer data types
      .load(getRelativeResourcePath()).as[Person]
    persons.collect() should contain theSameElementsAs (personList)
  }

  it should "read a csv Person file with first row as header" in {
    import sqlContext.implicits._
    val persons = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true") // Automatically infer data types
      .load(getRelativeResourcePath()).as[Person]
    persons.collect() should contain theSameElementsAs (List(
      Person("Giuseppe", "Garibaldi", 99),
      Person("Nanni", "Moretti", 32),
      Person("Quentin", "Tarantino", 88),
      Person("Antonio", "Murgia", 12),
      Person("Mickey", "Mouse", 19),
      Person("Giovanni", "Neri", 105)))
  }
}
