package com.collective.analytics

import com.collective.analytics.schema.ImpressionLog
import com.collective.analytics.schema.RowSyntax._
import org.apache.spark.sql.HLLFunctions._
import org.apache.spark.sql.Row
import org.scalatest.FlatSpec

class HyperLogLogSpec  extends FlatSpec with EmbeddedSparkContext {

  private val impressions = Seq(
    Row("bmw", "forbes.com", "cookie#1", 10L, 1L, Array("mk.a", "mk.b", "mk.c")),
    Row("bmw", "forbes.com", "cookie#2", 5L, 2L, Array("mk.a", "mk.e", "mk.f")),
    Row("bmw", "auto.com", "cookie#3", 7L, 0L, Array("mk.a", "mk.g", "mk.h"))
  )

  private val impressionLog =
    sqlContext.createDataFrame(sc.parallelize(impressions), ImpressionLog.schema)

  "HyperLogLog" should "calculate correct cardinlities" in {
    val cookiesHLL = impressionLog.select(hyperLogLog(ImpressionLog.cookie_id)).first().read[HLLColumn](0)
    assert(cookiesHLL.size() == 3)

    val sitesHLL = impressionLog.select(hyperLogLog(ImpressionLog.site_id)).first().read[HLLColumn](0)
    assert(sitesHLL.size() == 2)
  }

}
