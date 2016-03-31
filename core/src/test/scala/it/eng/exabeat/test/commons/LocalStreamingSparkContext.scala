package it.eng.exabeat.test.commons

import java.nio.file.Files

import org.apache.spark.streaming.{ClockWrapper, Seconds, StreamingContext}
import org.scalatest.Suite

/**
  * This is a commodity trait to be mixed to scalatest suites in order to have a freshly instantiated
  * StreamingSparkContext and a SparkContext during the test run accessible using the sparkContext and
  * streamingSparkContext methods.
  *
  * @author Antonio Murgia 
  * @version 25/03/16
  */
trait LocalStreamingSparkContext extends LocalSparkContext {
  this: Suite =>

  private var _ssc: StreamingContext = _

  def streamingSparkContext = _ssc

  private var _clock: ClockWrapper = _

  def clock = _clock

  val batchDuration = Seconds(1)

  val checkpointDir = Files.createTempDirectory(this.getClass.getSimpleName)

  override def beforeAll() = {
    sparkConf.set("spark.streaming.clock", "org.apache.spark.streaming.util.ManualClock")
    super.beforeAll()
    _ssc = new StreamingContext(sparkContext, batchDuration)
    _ssc.checkpoint(checkpointDir.toString)
    _clock = new ClockWrapper(streamingSparkContext)
  }

  override def afterAll() = {
    if (_ssc != null)
      _ssc.stop(stopSparkContext = false, stopGracefully = false)
    super.afterAll()
  }
}

