package com.collective.analytics

import com.adroll.cantor.HLLCounter

case class AudienceEstimate(
  ads: Vector[String],
  sites: Vector[String],
  cookiesHLL: HLLCounter,
  impressions: Long,
  clicks: Long
)

case class SegmentsEstimate(
  segments: Vector[String],
  cookiesHLL: HLLCounter,
  impressions: Long,
  clicks: Long)

trait AudienceAnalytics {

  /**
   * Estimate audience size for ads and sites
   */
  def audienceEstimate(ads: Vector[String] = Vector.empty, sites: Vector[String] = Vector.empty): AudienceEstimate

  /**
   * Estimate audience for segments
   */
  def segmentsEstimate(segments: Vector[String] = Vector.empty): SegmentsEstimate

  /**
   * Calculate audience intersection with all segments
   */
  def segmentsIntersection(audience: AudienceEstimate): Vector[(String, Long)]

  /**
   * Calculate audience intersection between aggregated estimates
   */
  def intersection(audience: AudienceEstimate, segments: SegmentsEstimate): Long = {
    HLLCounter.intersect(audience.cookiesHLL, segments.cookiesHLL)
  }

}
