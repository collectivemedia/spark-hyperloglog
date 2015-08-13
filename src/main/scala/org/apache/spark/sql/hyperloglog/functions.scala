package org.apache.spark.sql.hyperloglog

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.functions._

import scala.language.implicitConversions

object functions {

  private[this] implicit def toColumn(expr: Expression): Column = Column(expr)

  def hyperLogLog(e: Column): Column = HyperLogLog(e.expr)

  def hyperLogLog(e: String): Column = hyperLogLog(col(e))

  def mergeHyperLogLog(e: Column): Column = MergeHyperLogLog(e.expr)

  def mergeHyperLogLog(e: String): Column = mergeHyperLogLog(col(e))
}
