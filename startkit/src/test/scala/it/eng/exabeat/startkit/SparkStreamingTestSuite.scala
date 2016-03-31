package it.eng.exabeat.startkit

import it.eng.exabeat.test.commons.LocalStreamingSparkContext
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext, Time}
import org.scalatest.concurrent.Eventually
import org.scalatest._
import org.scalatest.time.{Millis, Span}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * @author Antonio Murgia 
  * @version 25/03/16
  */
class SparkStreamingTestSuite extends FlatSpec with LocalStreamingSparkContext with GivenWhenThen with Matchers with Eventually {
  private val windowDuration = Seconds(4)
  private val slideDuration = Seconds(2)

  // default timeout for eventually trait
  implicit override val patienceConfig =
    PatienceConfig(timeout = scaled(Span(1500, Millis)))


  it should "be counted" in {
    Given("streaming context is initialized")
    val lines = mutable.Queue[RDD[String]]()

    var results = ListBuffer.empty[Array[WordCount]]

    WordCount.count(streamingSparkContext, streamingSparkContext.queueStream(lines), windowDuration, slideDuration) { (wordsCount: RDD[WordCount], time: Time) =>
      results += wordsCount.collect()
    }

    streamingSparkContext.start()

    When("first set of words queued")
    lines += sparkContext.makeRDD(Seq("a", "b"))

    Then("words counted after first slide")
    clock.advance(slideDuration.milliseconds)
    eventually {
      results.last should equal(Array(
        WordCount("a", 1),
        WordCount("b", 1)))
    }

    When("second set of words queued")
    lines += sparkContext.makeRDD(Seq("b", "c"))

    Then("words counted after second slide")
    clock.advance(slideDuration.milliseconds)
    eventually {
      results.last should equal(Array(
        WordCount("a", 1),
        WordCount("b", 2),
        WordCount("c", 1)))
    }

    When("nothing more queued")

    Then("word counted after third slide")
    clock.advance(slideDuration.milliseconds)
    eventually {
      results.last should equal(Array(
        WordCount("a", 0),
        WordCount("b", 1),
        WordCount("c", 1)))
    }

    When("nothing more queued")

    Then("word counted after fourth slide")
    clock.advance(slideDuration.milliseconds)
    eventually {
      results.last should equal(Array(
        WordCount("a", 0),
        WordCount("b", 0),
        WordCount("c", 0)))
    }
  }





}
case class WordCount(word: String, count: Int)

object WordCount extends Serializable {

  type WordHandler = (RDD[WordCount], Time) => Unit

  def count(sc: SparkContext, lines: RDD[String]): RDD[WordCount] = count(sc, lines, Set())

  def count(sc: SparkContext, lines: RDD[String], stopWords: Set[String]): RDD[WordCount] = {
    val stopWordsVar = sc.broadcast(stopWords)

    val words = prepareWords(lines, stopWordsVar)

    val wordCounts = words.map(word => (word, 1)).reduceByKey(_ + _).map {
      case (word: String, count: Int) => WordCount(word, count)
    }

    val sortedWordCounts = wordCounts.sortBy(_.word)

    sortedWordCounts
  }

  def count(ssc: StreamingContext,
            lines: DStream[String],
            windowDuration: Duration,
            slideDuration: Duration)
           (handler: WordHandler): Unit = count(ssc, lines, windowDuration, slideDuration, Set())(handler)

  def count(ssc: StreamingContext,
            lines: DStream[String],
            windowDuration: Duration,
            slideDuration: Duration,
            stopWords: Set[String])
           (handler: WordHandler): Unit = {

    val sc = ssc.sparkContext
    val stopWordsVar = sc.broadcast(stopWords)

    val words = lines.transform(prepareWords(_, stopWordsVar))

    val wordCounts = words.map(x => (x, 1)).reduceByKeyAndWindow(_ + _, _ - _, windowDuration, slideDuration).map {
      case (word: String, count: Int) => WordCount(word, count)
    }

    wordCounts.foreachRDD((rdd: RDD[WordCount], time: Time) => {
      handler(rdd.sortBy(_.word), time)
    })
  }

  private def prepareWords(lines: RDD[String], stopWords: Broadcast[Set[String]]): RDD[String] = {
    lines.flatMap(_.split("\\s"))
      .map(_.replaceAll(",\\.", "").toLowerCase)
      .filter(!stopWords.value.contains(_)).filter(!_.isEmpty)
  }
}