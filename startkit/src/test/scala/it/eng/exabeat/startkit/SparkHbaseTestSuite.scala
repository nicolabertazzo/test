package it.eng.exabeat.startkit

import it.eng.exabeat.startkit.model.Person
import it.eng.exabeat.test.commons.{ResourceAccessor, TempDirSupport}
import org.scalatest.{FlatSpec, Matchers}
import org.apache.hadoop.hbase.{HBaseConfiguration, HBaseTestingUtility, HTableDescriptor, TableName}
import java.util.UUID

import org.apache.hadoop.hbase.client.ConnectionFactory

class SparkHbaseTestSuite extends FlatSpec with Matchers with ResourceAccessor with TempDirSupport {
//  lazy val personList = List(
//    Person("Giuseppe", "Garibaldi", 99),
//    Person("Nanni", "Moretti", 32),
//    Person("Quentin", "Tarantino", 88),
//    Person("Antonio", "Murgia", 12),
//    Person("Mickey", "Mouse", 19),
//    Person("Giovanni", "Neri", 105))
//
//  val tables: Seq[String] = Seq(UUID.randomUUID().toString)
//  val columnFamilies: Seq[String] = Seq(UUID.randomUUID().toString)
//
//  it should "read data" in {
//
//    val conf = HBaseConfiguration.create()
//
//    val connection = ConnectionFactory.createConnection(conf)
//
//    val admin = connection.getAdmin
//
//    val tableName = "test"
//    if (!admin.isTableAvailable(TableName.valueOf(tableName))) {
//      val tableDesc = new HTableDescriptor(TableName.valueOf(tableName))
//      admin.createTable(tableDesc)
//    }
//    val data = sparkContext.parallelize(personList)
//    data.toHBaseTable(tables.head).inColumnFamily(columnFamilies.head).save()
//    val read = sparkContext.hbaseTable[Person](tables.head).inColumnFamily(columnFamilies.head)
//    read.collect should contain theSameElementsAs (personList)
//
//  }
  it should "start a mini cluster" in {

    val testUtil = new HBaseTestingUtility()
    val conf = testUtil.getConfiguration()
    conf.set("hbase.rootdir", createTempDir() + "/hbase")
    conf.set("hbase.tmp.dir", createTempDir() + "/hbase_temp")
    testUtil.getConfiguration().reloadConfiguration()
    // start mini hbase cluster
    testUtil.startMiniCluster(1);
//    val cluster = utility.startMiniCluster(1)
//    val connection = utility.getConnection
//    val admin = connection.getAdmin

  }
}