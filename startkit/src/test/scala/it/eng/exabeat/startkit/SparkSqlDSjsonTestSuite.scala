package it.eng.exabeat.startkit

import it.eng.exabeat.startkit.model.Person
import it.eng.exabeat.test.commons.{LocalSqlSparkContext, ResourceAccessor, TempDirSupport}
import org.scalatest.{FlatSpec, Matchers}

/**
  *
  * @author Antonio Murgia
  * @version 25/03/16
  */
class SparkSqlDSjsonTestSuite extends FlatSpec with Matchers with ResourceAccessor with LocalSqlSparkContext with TempDirSupport {
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
    personList.toDS().toDF().write.json(outputFile)
    val persons = sqlContext.read
      .json(outputFile).as[Person]
    persons.collect() should contain theSameElementsAs (personList)
  }

  it should "read a json Person file" in {
    import sqlContext.implicits._
    val persons = sqlContext.read
      .json(getRelativeResourcePath()).as[Person]
    persons.collect() should contain theSameElementsAs (personList)
  }
}
