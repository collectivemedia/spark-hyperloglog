package com.collective.analytics

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}


object EmbeddedSparkContext {

  private[this] val conf =
    new SparkConf()
      .setMaster("local[2]")
      .set("spark.local.ip","localhost")
      .set("spark.driver.host","localhost")
      .setAppName("Interactive Audience Analytics")

  lazy val sc: SparkContext = new SparkContext(conf)

  lazy val sqlContext: SQLContext = new SQLContext(sc)

}


trait EmbeddedSparkContext {

  def sc: SparkContext = EmbeddedSparkContext.sc

  implicit def sqlContext: SQLContext = EmbeddedSparkContext.sqlContext

  def waitFor[T](f: Future[T], timeout: Duration = 5.second): T = {
    Await.result(f, timeout)
  }

}
