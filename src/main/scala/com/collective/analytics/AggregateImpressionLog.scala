package com.collective.analytics

import com.collective.analytics.schema.{ActivityLog, SegmentLog, ImpressionLog}
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

class AggregateImpressionLog(impressionLog: DataFrame) extends Serializable {
  private val log = LoggerFactory.getLogger(classOf[AggregateImpressionLog])

  /**
   * Remove segment information and group all
   * unique cookie_id into HyperLogLog object
   *
   * @return Activity Log DataFrame
   */
  def activityLog(): DataFrame = {
    log.info(s"Compute activity log")

    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.HyperLogLogFn._

    impressionLog.groupBy(
      col(ImpressionLog.ad_id)             as ActivityLog.ad_id,
      col(ImpressionLog.site_id)           as ActivityLog.site_id
    ).agg(
      hyperLogLog(ImpressionLog.cookie_id) as ActivityLog.cookies_hll,
      sum(ImpressionLog.impressions)       as ActivityLog.impressions,
      sum(ImpressionLog.clicks)            as ActivityLog.clicks
    )
  }

  /**
   * Remove site and and information abd group all
   * unique cookie_id into HyperLogLog object
   *
   * @return Segment Log DataFrae
   */
  def segmentLog(): DataFrame = {
    log.info(s"Compute segment log")

    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.HyperLogLogFn._

    impressionLog.select(
      col(ImpressionLog.ad_id),
      col(ImpressionLog.site_id),
      col(ImpressionLog.cookie_id),
      col(ImpressionLog.impressions),
      col(ImpressionLog.clicks),
      explode(col(ImpressionLog.segments))   as SegmentLog.segment
    ).groupBy(
        col(SegmentLog.segment)
      ).agg(
        hyperLogLog(ImpressionLog.cookie_id) as SegmentLog.cookies_hll,
        sum(ImpressionLog.impressions)       as SegmentLog.impressions,
        sum(ImpressionLog.clicks)            as SegmentLog.clicks
      )
  }

}
