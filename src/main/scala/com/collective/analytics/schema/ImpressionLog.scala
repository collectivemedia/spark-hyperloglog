package com.collective.analytics.schema

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

case class ImpressionLog(
  adId: String,
  siteId: String,
  cookieId: String,
  impressions: Long,
  clicks: Long,
  segments: Array[String]
)

object ImpressionLog { self =>

  val ad_id               = "ad_id"
  val site_id             = "site_id"
  val cookie_id           = "cookie_id"
  val impressions         = "impressions"
  val clicks              = "clicks"
  val segments            = "segments"

  object Schema extends SchemaDefinition {
    val ad_id        = structField(self.ad_id,       StringType)
    val site_id      = structField(self.site_id,     StringType)
    val cookie_id    = structField(self.cookie_id,   StringType)
    val impressions  = structField(self.impressions, LongType)
    val clicks       = structField(self.clicks,      LongType)
    val segments     = structField(self.segments,    ArrayType(StringType))
  }

  val schema: StructType = StructType(Schema.fields)

  import RowSyntax._

  def parse(row: Row): ImpressionLog = ImpressionLog(
    row.read[StringColumn](0),
    row.read[StringColumn](1),
    row.read[StringColumn](2),
    row.read[LongColumn](3),
    row.read[LongColumn](4),
    row.read[StringArrayColumn](5)
  )

}
