package com.collective.analytics

import com.collective.analytics.schema.{SegmentLog, ActivityLog, ImpressionLog}
import org.apache.spark.sql.Row
import org.scalatest.FlatSpec

class AggregateImpressionLogSpec extends FlatSpec with EmbeddedSparkContext {

  private val impressions = Seq(
    Row("bmw", "forbes.com", "cookie#1", 10L, 1L, Array("income:50000", "education:high-school", "interest:technology")),
    Row("bmw", "forbes.com", "cookie#2", 5L, 2L, Array("income:50000", "education:college", "interest:auto")),
    Row("bmw", "auto.com", "cookie#3", 7L, 0L, Array("income:100000", "education:high-school", "interest:auto"))
  )

  private val impressionLog =
    sqlContext.createDataFrame(sc.parallelize(impressions), ImpressionLog.schema)

  private val aggregate = new AggregateImpressionLog(impressionLog)

  "AggregateImpressionLog" should "build activity log" in {
    val activityLog = aggregate.activityLog().collect().map(ActivityLog.parse)

    assert(activityLog.length == 2)

    val bmwAtForbes = activityLog.find(r => r.adId == "bmw" && r.siteId == "forbes.com").get
    assert(bmwAtForbes.cookiesHLL.size() == 2)
    assert(bmwAtForbes.impressions == 15)
    assert(bmwAtForbes.clicks == 3)

    val bmwAtAuto = activityLog.find(r => r.adId == "bmw" && r.siteId == "auto.com").get
    assert(bmwAtAuto.cookiesHLL.size() == 1)
    assert(bmwAtAuto.impressions == 7)
    assert(bmwAtAuto.clicks == 0)
  }

  it should "build segment log" in {
    val segmentLog = aggregate.segmentLog().collect().map(SegmentLog.parse)

    assert(segmentLog.length == 6)

    val income50k = segmentLog.find(_.segment == "income:50000").get
    assert(income50k.cookiesHLL.size() == 2)
    assert(income50k.impressions == 15)
    assert(income50k.clicks == 3)
  }

}
