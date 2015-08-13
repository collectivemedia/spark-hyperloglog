package com.collective.analytics

import com.collective.analytics.schema.ImpressionLog

object AudienceAnalyticsApp extends App with EmbeddedSparkContext with DataGenerator {

  // Generate impression log
  val impressions =
    repeat(100, impressionRow("bmw",  "forbes.com", 10L, 1L, Array("income:50000",  "education:high-school", "interest:technology")))   ++
    repeat(100, impressionRow("bmw",  "forbes.com", 5L,  2L, Array("income:50000",  "education:college",     "interest:auto")))         ++
    repeat(100, impressionRow("bmw",  "auto.com",   7L,  0L, Array("income:100000", "education:high-school", "interest:auto")))         ++
    repeat(100, impressionRow("audi", "cnn.com",    2L,  0L, Array("income:50000",  "interest:audi",         "education:high-school")))


  // Create Impresssion Log DataFrame
  // In real world app it might look like this: sqlContext.read.parquet("hdfs://impression_log")
  val impressionLog = sqlContext.createDataFrame(sc.parallelize(impressions), ImpressionLog.schema)

  val aggregate = new AggregateImpressionLog(impressionLog)
  val analytics = new SparkAudienceAnalytics(aggregate)

  // How many people in bmw campaign

  val bmwEstimate = analytics.audienceEstimate(ads = Vector("bmw"))

  println(s"BMW campaign audience estimate: ${bmwEstimate.cookiesHLL.size()}. " +
    s"Impressions: ${bmwEstimate.impressions}. Clicks: ${bmwEstimate.clicks}")

  // How many people with income:50000

  val fiftykEstimate = analytics.segmentsEstimate(segments = Vector("income:50000"))
  println(s"Income:50000 segment estimate: ${fiftykEstimate.cookiesHLL.size()}. " +
    s"Impressions: ${fiftykEstimate.impressions}. Clicks: ${fiftykEstimate.clicks}")

  // BMW campaign intersection with all segments

  val bmwIntersection = analytics.segmentsIntersection(bmwEstimate)

  println(s"BMW audience intersection:")
  bmwIntersection.sortBy(_._1).foreach { case (segment, intersection) =>
      println(s" - segment: $segment. intersection: $intersection")
  }

}
