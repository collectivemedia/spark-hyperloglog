package com.collective.analytics

import com.collective.analytics.schema.ImpressionLog
import org.apache.spark.sql.Row
import org.scalatest.{FlatSpec, ShouldMatchers}


class SparkAudienceAnalyticsSpec extends AudienceAnalyticsSpec with EmbeddedSparkContext {
  def builder: Vector[Row] => AudienceAnalytics =
    log => new SparkAudienceAnalytics(
      new AggregateImpressionLog(sqlContext.createDataFrame(sc.parallelize(log), ImpressionLog.schema))
    )
}

class InMemoryAudienceAnalyticsSpec extends AudienceAnalyticsSpec {
  def builder: Vector[Row] => AudienceAnalytics =
    log => new InMemoryAudienceAnalytics(log.map(ImpressionLog.parse))
}

abstract class AudienceAnalyticsSpec extends FlatSpec with ShouldMatchers with DataGenerator {

  def builder: Vector[Row] => AudienceAnalytics

  private val impressions =
    repeat(100, impressionRow("bmw", "forbes.com", 10L, 1L, Array("income:50000", "education:high-school", "interest:technology"))) ++
    repeat(100, impressionRow("bmw", "forbes.com", 5L, 2L, Array("income:50000", "education:college", "interest:auto"))) ++
    repeat(100, impressionRow("bmw", "auto.com", 7L, 0L, Array("income:100000", "education:high-school", "interest:auto"))) ++
    repeat(100, impressionRow("audi", "cnn.com", 2L, 0L, Array("income:50000", "interest:audi", "education:high-school")))

  //private val impressionLog = impressions.map(ImpressionLog.parse)

  private val analytics = builder(impressions)

  "InMemoryAudienceAnalytics" should "compute audience estimate" in {
    val bmwEstimate = analytics.audienceEstimate(Vector("bmw"))
    assert(bmwEstimate.cookiesHLL.size() == 3 * 100)
    assert(bmwEstimate.impressions == 22 * 100)
    assert(bmwEstimate.clicks == 3 * 100)

    val forbesEstimate = analytics.audienceEstimate(sites = Vector("forbes.com"))
    assert(forbesEstimate.cookiesHLL.size() == 2 * 100)
    assert(forbesEstimate.impressions == 15 * 100)
    assert(forbesEstimate.clicks == 3 * 100)
  }

  it should "compute segment estimate" in {
    val fiftyK = analytics.segmentsEstimate(Vector("income:50000"))
    assert(fiftyK.cookiesHLL.size() == 3 * 100)
    assert(fiftyK.impressions == 17 * 100)
    assert(fiftyK.clicks == 3 * 100)

    val highSchool = analytics.segmentsEstimate(Vector("education:high-school"))
    assert(highSchool.cookiesHLL.size() == 3 * 100)
    assert(highSchool.impressions == 19 * 100)
    assert(highSchool.clicks == 1 * 100)
  }

  it should "compute audience intersection" in {
    val bmwAudience = analytics.audienceEstimate(Vector("bmw"))
    val intersection = analytics.segmentsIntersection(bmwAudience).toMap

    assert(intersection.size == 7)
    assert(intersection("interest:audi") == 0)
    intersection("income:50000") should (be >= 190L and be <= 2010L)
  }

}
