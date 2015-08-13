package com.collective.analytics

import java.util.UUID

import org.apache.spark.sql.Row

trait DataGenerator {

  def cookieId = UUID.randomUUID().toString

  def impressionRow(ad: String, site: String, impressions: Long, clicks: Long, segments: Array[String]): Row =
   Row(ad, site, cookieId, impressions, clicks, segments)

  def repeat[T](n: Int, el: =>T): Vector[T] = {
      Stream.continually(el).take(n).toVector
  }

}
