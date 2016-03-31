package it.eng.exabeat.startkit.model




/**
  * @author Antonio Murgia 
  * @version 25/03/16
  */
case class Person(name: String, surname: String, age: Long){
  def id() = {
    import java.security.MessageDigest
    MessageDigest.getInstance("MD5").digest((name + surname).getBytes())
  }
}

object PersonFormat {
  import it.nerdammer.spark.hbase.conversion.FieldWriter
  import org.apache.hadoop.hbase.util.Bytes
  import it.nerdammer.spark.hbase.conversion.FieldReader

  val columnNames = Seq("name", "surname", "age")

  implicit def myDataWriter: FieldWriter[Person] = new FieldWriter[Person] {
    override def map(data: Person): HBaseData =
      Seq(
        Some(data.id()),
        Some(Bytes.toBytes(data.name)),
        Some(Bytes.toBytes(data.surname)),
        Some(Bytes.toBytes(data.age))
      )

    override def columns = columnNames
  }

  implicit def myDataReader: FieldReader[Person] = new FieldReader[Person] {
    override def map(data: HBaseData): Person = Person(
      name = Bytes.toString(data.drop(1).head.get),
      surname = Bytes.toString(data.drop(2).head.get),
      age = Bytes.toLong(data.drop(3).head.get)
    )
    override def columns = columnNames
  }
}

