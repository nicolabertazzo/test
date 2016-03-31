package it.eng.exabeat.startkit

import it.eng.exabeat.startkit.model.Person
import it.eng.exabeat.test.commons.{LocalSqlSparkContext, ResourceAccessor, TempDirSupport}
import org.scalatest.{FlatSpec, Matchers}

/**
  * parquet files fields can be accessed using both the column name or the column position (array-like)
  * @author Antonio Murgia 
  * @version 25/03/16
  */
class SparkSqlDFparquetTestSuite extends FlatSpec with Matchers with ResourceAccessor with LocalSqlSparkContext with TempDirSupport {
  lazy val personList = List(
    Person("Giuseppe", "Garibaldi", 99),
    Person("Nanni", "Moretti", 32),
    Person("Quentin", "Tarantino", 88),
    Person("Antonio", "Murgia", 12),
    Person("Mickey", "Mouse", 19),
    Person("Giovanni", "Neri", 105))

  it should "write a parquet Person file" in {
    import sqlContext.implicits._
    val outputFile = createTempDir().getAbsolutePath + "output"
    personList.toDF().write.parquet(outputFile)
    val persons = sqlContext.read
      .parquet(outputFile)
      .map { row =>
        Person(row.getString(0), row.getString(1), row.getLong(2))
      }
    persons.collect() should contain theSameElementsAs (personList)
  }

  it should "read a parquet Person file" in {
    sqlContext.read
      .parquet(getRelativeResourcePath())
      .map { row =>
        Person(row.getAs[String]("name"), row.getAs[String]("surname"), row.getAs[Long]("age"))
      }.collect() should contain theSameElementsAs (personList)
  }
}
