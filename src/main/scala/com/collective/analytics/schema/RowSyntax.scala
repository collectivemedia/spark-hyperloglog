package com.collective.analytics.schema

import org.apache.spark.sql
import org.apache.spark.sql.{MergeHyperLogLog, Row}

object RowSyntax {

  sealed trait ColumnType

  trait IntColumn extends ColumnType
  trait LongColumn extends ColumnType
  trait StringColumn extends ColumnType
  trait BinaryColumn extends ColumnType
  trait HLLColumn extends ColumnType

  sealed trait ColumnReader[C <: ColumnType] { self =>
    type Out

    def read(row: sql.Row)(idx: Int): Out

    def map[Out1](f: Out => Out1): ColumnReader[C] {type Out = Out1} =
      new ColumnReader[C] {
        type Out = Out1

        def read(row: Row)(idx: Int): Out = {
          f(self.read(row)(idx))
        }
      }
  }

  implicit class RowOps(val row: Row) extends AnyVal {
    def read[C <: ColumnType](idx: Int)(implicit reader: ColumnReader[C]): reader.Out = {
      reader.read(row)(idx)
    }
  }

  class IntReader[C <: ColumnType] extends ColumnReader[C] {
    type Out = Int
    def read(row: Row)(idx: Int): Out = row.getInt(idx)
  }

  class LongReader[C <: ColumnType] extends ColumnReader[C] {
    type Out = Long
    def read(row: Row)(idx: Int): Out = row.getLong(idx)
  }

  class StringReader[C <: ColumnType] extends ColumnReader[C] {
    type Out = String
    def read(row: Row)(idx: Int): Out = row(idx) match {
      case null => ""
      case str: String => str
      case arr: Array[_] => new String(arr.asInstanceOf[Array[Byte]])
    }
  }

  class BinaryReader[C <: ColumnType] extends ColumnReader[C] {
    type Out = Array[Byte]

    def read(row: Row)(idx: Int): Out = {
      row.getAs[Array[Byte]](idx)
    }
  }

  // Implicit Column Readers

  implicit val intReader = new IntReader[IntColumn]
  implicit val longReader = new LongReader[LongColumn]
  implicit val stringReader = new StringReader[StringColumn]
  implicit val binaryReader = new BinaryReader[BinaryColumn]

  implicit val cardinalityReader = new BinaryReader[HLLColumn] map { bytes =>
    MergeHyperLogLog.readHLLWritable(bytes).get()
  }

}
