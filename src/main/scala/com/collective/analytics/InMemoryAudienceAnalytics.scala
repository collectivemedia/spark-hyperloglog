package com.collective.analytics

import com.collective.analytics.schema.ImpressionLog
import org.apache.spark.sql.HyperLogLogConfig

class InMemoryAudienceAnalytics(impressions: Vector[ImpressionLog]) extends AudienceAnalytics {

  // Unique segments in impression log
  private val segments: Set[String] = impressions.flatMap(_.segments.toVector).toSet

  def audienceEstimate(ads: Vector[String], sites: Vector[String]): AudienceEstimate = {
    val (cnt, i, c) = impressions
      .filter(impression => ads.contains(impression.adId) || sites.contains(impression.siteId))
      .foldLeft((HyperLogLogConfig.zeroHLL, 0L, 0L)) { case ((counter, imp, clk), log) =>
        counter.put(log.cookieId)
        (counter, imp + log.impressions, clk + log.clicks)
      }
    AudienceEstimate(ads, sites, cnt, i, c)
  }

  def segmentsEstimate(segments: Vector[String]): SegmentsEstimate = {
    val (cnt, i, c) = impressions
      .filter(impression => segments.intersect(impression.segments).nonEmpty)
      .foldLeft((HyperLogLogConfig.zeroHLL, 0L, 0L)) { case ((counter, imp, clk), log) =>
      counter.put(log.cookieId)
      (counter, imp + log.impressions, clk + log.clicks)
    }
    SegmentsEstimate(segments, cnt, i, c)
  }

  def segmentsIntersection(audience: AudienceEstimate): Vector[(String, Long)] = {
    segments.toVector.map { segment =>
      (segment, intersection(audience, segmentsEstimate(Vector(segment))))
    }
  }
}
