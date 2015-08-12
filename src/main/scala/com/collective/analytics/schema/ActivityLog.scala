package com.collective.analytics.schema

import com.adroll.cantor.HLLCounter
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

case class ActivityLog(
  adId: String,
  siteId: String,
  cookiesHLL: HLLCounter,
  impressions: Long,
  clicks: Long
)

object ActivityLog { self =>

  val ad_id               = "ad_id"
  val site_id             = "site_id"
  val cookies_hll         = "cookies_hll"
  val impressions         = "impressions"
  val clicks              = "clicks"

  object Schema extends SchemaDefinition {
    val ad_id        = structField(self.ad_id,       StringType)
    val site_id      = structField(self.site_id,     StringType)
    val cookies_hll  = structField(self.cookies_hll, BinaryType)
    val impressions  = structField(self.impressions, LongType)
    val clicks       = structField(self.clicks,      LongType)
  }

  val schema: StructType = StructType(Schema.fields)

  import RowSyntax._

  def parse(row: Row): ActivityLog = {
    ActivityLog(
      row.read[StringColumn](0),
      row.read[StringColumn](1),
      row.read[HLLColumn](2),
      row.read[LongColumn](3),
      row.read[LongColumn](4)
    )
  }

}
