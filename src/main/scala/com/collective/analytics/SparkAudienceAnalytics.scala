package com.collective.analytics

import com.adroll.cantor.HLLCounter
import com.collective.analytics.schema.{SegmentLog, ActivityLog}


class SparkAudienceAnalytics(aggregate: AggregateImpressionLog) extends AudienceAnalytics { self =>

  private lazy val activity = aggregate.activityLog()
  private lazy val segments = aggregate.segmentLog()

  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.HyperLogLogFn._
  import com.collective.analytics.schema.RowSyntax._

  def audienceEstimate(ads: Vector[String], sites: Vector[String]): AudienceEstimate = {
    val row = activity.filter(
      (col(ActivityLog.ad_id)   in (ads.map(lit): _*))     or
      (col(ActivityLog.site_id) in (sites.map(lit): _*))
    ).select(
        mergeHyperLogLog(col(ActivityLog.cookies_hll)),
        sum(col(ActivityLog.impressions)),
        sum(col(ActivityLog.clicks))
      ).first()

    AudienceEstimate(ads, sites,
      row.read[HLLColumn](0),
      row.read[LongColumn](1),
      row.read[LongColumn](2)
    )
  }

  def segmentsEstimate(segments: Vector[String]): SegmentsEstimate = {
    val row = self.segments.filter(
      col(SegmentLog.segment) in (segments.map(lit): _*)
    ).select(
        mergeHyperLogLog(col(SegmentLog.cookies_hll)),
        sum(col(SegmentLog.impressions)),
        sum(col(SegmentLog.clicks))
      ).first()

    SegmentsEstimate(segments,
      row.read[HLLColumn](0),
      row.read[LongColumn](1),
      row.read[LongColumn](2)
    )
  }

  def segmentsIntersection(audience: AudienceEstimate): Vector[(String, Long)] = {
    val intersection = segments.select(
      col(SegmentLog.segment), col(SegmentLog.cookies_hll)
    ).map { row =>
      val segment = row.read[StringColumn](0)
      val cookies = row.read[HLLColumn](1)
      (segment, HLLCounter.intersect(audience.cookiesHLL, cookies))
    }
    intersection.collect().toVector
  }
}
