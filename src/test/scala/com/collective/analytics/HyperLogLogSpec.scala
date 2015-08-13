package com.collective.analytics

import com.collective.analytics.schema.ImpressionLog
import com.collective.analytics.schema.RowSyntax._
import org.apache.spark.sql.HyperLogLogFn._
import org.apache.spark.sql.Row
import org.scalatest.FlatSpec

class HyperLogLogSpec  extends FlatSpec with EmbeddedSparkContext {

  private val impressions = Seq(
    Row("bmw", "forbes.com", "cookie#1", 10L, 1L, Array("income:50000", "education:high-school", "interest:technology")),
    Row("bmw", "forbes.com", "cookie#2", 5L, 2L, Array("income:150000", "education:college", "interest:auto")),
    Row("bmw", "auto.com", "cookie#3", 7L, 0L, Array("income:100000", "education:phd", "interest:music"))
  )

  private val impressionLog =
    sqlContext.createDataFrame(sc.parallelize(impressions), ImpressionLog.schema)

  "HyperLogLog" should "calculate correct column cardinalities" in {
    val cookiesHLL = impressionLog.select(hyperLogLog(ImpressionLog.cookie_id)).first().read[HLLColumn](0)
    assert(cookiesHLL.size() == 3)

    val sitesHLL = impressionLog.select(hyperLogLog(ImpressionLog.site_id)).first().read[HLLColumn](0)
    assert(sitesHLL.size() == 2)
  }

}
