package com.collective.analytics.schema

import com.adroll.cantor.HLLCounter
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

case class SegmentLog(
  segment: String,
  cookiesHLL: HLLCounter,
  impressions: Long,
  clicks: Long
)

object SegmentLog { self =>

  val segment             = "segment"
  val cookies_hll         = "cookies_hll"
  val impressions         = "impressions"
  val clicks              = "clicks"

  object Schema extends SchemaDefinition {
    val segment      = structField(self.segment,     StringType)
    val cookies_hll  = structField(self.cookies_hll, BinaryType)
    val impressions  = structField(self.impressions, LongType)
    val clicks       = structField(self.clicks,      LongType)
  }

  val schema: StructType = StructType(Schema.fields)

  import RowSyntax._

  def parse(row: Row): SegmentLog = {
    SegmentLog(
      row.read[StringColumn](0),
      row.read[HLLColumn](1),
      row.read[LongColumn](2),
      row.read[LongColumn](3)
    )
  }

}
