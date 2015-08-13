package org.apache.spark.sql.hyperloglog

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import com.adroll.cantor.{HLLCounter, HLLWritable}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.trees
import org.apache.spark.sql.types.{BinaryType, DataType}

object HyperLogLogConfig {

  val p = 11
  val k = 2048

  def zeroHLL: HLLCounter = new HLLCounter(p.toByte, true, k)

}

trait HyperLogLogFormat {

  import sbinary.DefaultProtocol._
  import sbinary.Operations._
  import sbinary._

  val zeroHLLBytes = writeHLLWritable(new HLLWritable(HyperLogLogConfig.zeroHLL))

  @inline
  def readHLLWritable(bytes: Array[Byte]): HLLWritable = {
    val hllByteInput = new DataInputStream(new ByteArrayInputStream(bytes))
    val writable = new HLLWritable(-1, -1, -1, null, null)
    writable.readFields(hllByteInput)
    writable
  }

  @inline
  def writeHLLWritable(writable: HLLWritable): Array[Byte] = {
    val bytes = new ByteArrayOutputStream(writable.getSize())
    writable.write(new DataOutputStream(bytes))
    bytes.toByteArray
  }

  protected implicit val HLLWritableFormat: Format[HLLWritable] =
    new Format[HLLWritable] {
      @inline def reads(in: Input): HLLWritable =
        readHLLWritable(read[Array[Byte]](in))
      @inline def writes(out: Output, value: HLLWritable): Unit =
        write[Array[Byte]](out, writeHLLWritable(value))
    }
}

object MergeHyperLogLog extends HyperLogLogFormat

case class MergeHyperLogLogPartition(child: Expression)
  extends AggregateExpression with trees.UnaryNode[Expression] {

  override def nullable: Boolean = false
  override def dataType: DataType = child.dataType
  override def toString: String = s"MergeHyperLogLog($child)"
  override def newInstance(): AggregateFunction = new MergeHyperLogLogPartitionFunction(child, this)
}

case class MergeHyperLogLogMerge(child: Expression)
  extends AggregateExpression with trees.UnaryNode[Expression] {

  override def nullable: Boolean = false
  override def dataType: DataType = BinaryType
  override def toString: String = s"MergeHyperLogLog($child)"
  override def newInstance(): AggregateFunction = new MergeHyperLogLogMergeFunction(child, this)
}

case class MergeHyperLogLog(child: Expression)
  extends PartialAggregate with trees.UnaryNode[Expression] {

  override def nullable: Boolean = false
  override def dataType: DataType = BinaryType
  override def toString: String = s"MergeHyperLogLog($child)"

  override def asPartial: SplitEvaluation = {
    val partialCount =
      Alias(MergeHyperLogLogPartition(child), "PartialMergeHyperLogLog")()

    SplitEvaluation(
      MergeHyperLogLogMerge(partialCount.toAttribute),
      partialCount :: Nil)
  }

  override def newInstance(): AggregateFunction =
    throw new UnsupportedOperationException("MergeHyperLogLog.newInstance is unsupported")
}

case class MergeHyperLogLogPartitionFunction(
  expr: Expression,
  base: AggregateExpression)
  extends AggregateFunction {
  def this() = this(null, null) // Required for serialization.

  import MergeHyperLogLog._

  private var bytes: Array[Byte] = null
  private var writable: HLLWritable = null

  @inline
  override def update(input: Row): Unit = {
    val evaluatedExpr = expr.eval(input)
    if (evaluatedExpr != null) {
      if (bytes == null && writable == null) {
        // Got first HLL bytes
        bytes = evaluatedExpr.asInstanceOf[Array[Byte]]
      } else if (bytes != null && writable == null) {
        // Got second HLL bytes
        writable = readHLLWritable(bytes)
        writable = writable.combine(readHLLWritable(evaluatedExpr.asInstanceOf[Array[Byte]]))
        // nullify bytes
        bytes = null
      } else if (bytes == null && writable != null) {
        // Got 3+ HLL bytes
        writable = writable.combine(readHLLWritable(evaluatedExpr.asInstanceOf[Array[Byte]]))
      } else {
        sys.error(s"Unexpected state: bytes and writable are not null")
      }
    }
  }

  @inline
  override def eval(input: Row): Any = {
    if (bytes == null && writable == null) {
      zeroHLLBytes
    } else if (bytes != null && writable == null) {
      bytes
    } else if (bytes == null && writable != null) {
      writeHLLWritable(writable)
    } else {
      sys.error(s"Unexpected state: bytes and writable are not null")
    }
  }
}

case class MergeHyperLogLogMergeFunction(
  expr: Expression,
  base: AggregateExpression)
  extends AggregateFunction {
  def this() = this(null, null) // Required for serialization.

  import MergeHyperLogLog._

  private var bytes: Array[Byte] = null
  private var writable: HLLWritable = null

  @inline
  override def update(input: Row): Unit = {
    val evaluatedExpr = expr.eval(input)
    if (bytes == null && writable == null) {
      // Got first HLL bytes
      bytes = evaluatedExpr.asInstanceOf[Array[Byte]]
    } else if (bytes != null && writable == null) {
      // Got second HLL bytes
      writable = readHLLWritable(bytes)
      writable = writable.combine(readHLLWritable(evaluatedExpr.asInstanceOf[Array[Byte]]))
      // nullify bytes
      bytes = null
    } else if (bytes == null && writable != null) {
      // Got 3+ HLL bytes
      writable = writable.combine(readHLLWritable(evaluatedExpr.asInstanceOf[Array[Byte]]))
    } else {
      sys.error(s"Unexpected state: bytes and writable are not null")
    }
  }

  @inline
  override def eval(input: Row): Any = {
    if (bytes == null && writable == null) {
      zeroHLLBytes
    } else if (bytes != null && writable == null) {
      bytes
    } else if (bytes == null && writable != null) {
      writeHLLWritable(writable)
    } else {
      sys.error(s"Unexpected state: bytes and writable are not null")
    }
  }
}

object HyperLogLog extends HyperLogLogFormat

case class HyperLogLogPartition(child: Expression)
  extends AggregateExpression with trees.UnaryNode[Expression] {

  override def nullable: Boolean = false
  override def dataType: DataType = BinaryType
  override def toString: String = s"HyperLogLog($child)"
  override def newInstance(): AggregateFunction = new HyperLogLogPartitionFunction(child, this)
}

case class HyperLogLogMerge(child: Expression)
  extends AggregateExpression with trees.UnaryNode[Expression] {

  override def nullable: Boolean = false
  override def dataType: DataType = BinaryType
  override def toString: String = s"HyperLogLog($child)"
  override def newInstance(): AggregateFunction = new MergeHyperLogLogMergeFunction(child, this)
}

case class HyperLogLog(child: Expression)
  extends PartialAggregate with trees.UnaryNode[Expression] {

  override def nullable: Boolean = false
  override def dataType: DataType = BinaryType
  override def toString: String = s"HyperLogLog($child)"

  override def asPartial: SplitEvaluation = {
    val partialCount =
      Alias(HyperLogLogPartition(child), "PartialHyperLogLog")()

    SplitEvaluation(
      HyperLogLogMerge(partialCount.toAttribute),
      partialCount :: Nil)
  }

  override def newInstance(): AggregateFunction =
    throw new UnsupportedOperationException("HyperLogLog.newInstance is unsupported")
}

case class HyperLogLogPartitionFunction(
  expr: Expression,
  base: AggregateExpression)
  extends AggregateFunction {
  def this() = this(null, null) // Required for serialization.

  import HyperLogLog._

  private var counter: HLLCounter = null

  @inline
  override def update(input: Row): Unit = {
    val evaluatedExpr = expr.eval(input)
    if (evaluatedExpr != null) {
      if (counter == null) {
        // Got first element
        counter = HyperLogLogConfig.zeroHLL
      }
      counter.put(evaluatedExpr.toString)
    }
  }

  @inline
  override def eval(input: Row): Any = {
    if (counter == null) {
      zeroHLLBytes
    } else if (counter != null) {
      writeHLLWritable(new HLLWritable(counter))
    }
  }
}
