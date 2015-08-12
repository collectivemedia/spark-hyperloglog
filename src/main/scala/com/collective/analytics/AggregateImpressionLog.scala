package com.collective.analytics

import com.collective.analytics.schema.ImpressionLog
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
    import org.apache.spark.sql.HLLFunctions._

    impressionLog.groupBy(col(ImpressionLog.ad_id), col(ImpressionLog.site_id)).agg(
      hyperLogLog(ImpressionLog.cookie_id),
      sum(ImpressionLog.impressions),
      sum(ImpressionLog.clicks)
    )

  }

}

