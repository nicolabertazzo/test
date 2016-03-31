package it.eng.exabeat.startkit

import it.eng.exabeat.startkit.model.Person
import it.eng.exabeat.test.commons.{LocalSqlSparkContext, ResourceAccessor, TempDirSupport}
import org.scalatest.{FlatSpec, Matchers}

/**
  * json files fields read should be accessed using the column name
  * @author Antonio Murgia
  * @version 25/03/16
  */
class SparkSqlDFjsonTestSuite extends FlatSpec with Matchers with ResourceAccessor with LocalSqlSparkContext with TempDirSupport {
  lazy val personList = List(
    Person("Giuseppe", "Garibaldi", 99),
    Person("Nanni", "Moretti", 32),
    Person("Quentin", "Tarantino", 88),
    Person("Antonio", "Murgia", 12),
    Person("Mickey", "Mouse", 19),
    Person("Giovanni", "Neri", 105))

  it should "write a json Person file" in {
    import sqlContext.implicits._
    val outputFile = createTempDir().getAbsolutePath + "output"
    personList.toDF().write.json(outputFile)
    val persons = sqlContext.read
      .json(outputFile)
      .map { row =>
        Person(row.getAs[String]("name"), row.getAs[String]("surname"), row.getAs[Long]("age"))
      }
    persons.collect() should contain theSameElementsAs (personList)
  }

  it should "read a json Person file" in {
    import sqlContext.implicits._
    val persons = sqlContext.read
      .json(getRelativeResourcePath())
      .map { row =>
        Person(row.getAs[String]("name"), row.getAs[String]("surname"), row.getAs[Long]("age"))
      }
    persons.collect() should contain theSameElementsAs (personList)
  }
}
