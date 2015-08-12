package com.collective.analytics

import com.collective.analytics.schema.{ActivityLog, ImpressionLog}
import org.apache.spark.sql.Row
import org.scalatest.FlatSpec

class AggregateImpressionLogSpec extends FlatSpec with EmbeddedSparkContext {

  private val impressions = Seq(
    Row("bmw", "forbes.com", "cookie#1", 10L, 1L, Array("mk.a", "mk.b", "mk.c")),
    Row("bmw", "forbes.com", "cookie#2", 5L, 2L, Array("mk.a", "mk.e", "mk.f")),
    Row("bmw", "auto.com", "cookie#3", 7L, 0L, Array("mk.a", "mk.g", "mk.h"))
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

}
