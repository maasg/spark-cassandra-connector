package com.datastax.spark.connector.writer

import com.datastax.driver.core._
import org.apache.spark.Logging

private[connector] class BatchStatementBuilder(
    val batchType: BatchStatement.Type,
    val consistencyLevel: ConsistencyLevel,
    val maybeRoutingKeyGenerator: Option[RoutingKeyGenerator] = None
    ) extends Logging {

  /** Converts a sequence of statements into a batch if its size is greater than 1.
    * Sets the routing key and consistency level. */
  def maybeCreateBatch(stmts: Seq[RichBoundStatement]): RichStatement = {
    require(stmts.size > 0, "Statements list cannot be empty")
    val stmt = stmts.head
    maybeRoutingKeyGenerator.foreach{keyGen =>
      // for batch statements, it is enough to set routing key for the first statement
      stmt.setRoutingKey(keyGen.apply(stmt))
    }

    if (stmts.size == 1) {
      stmt.setConsistencyLevel(consistencyLevel)
      stmt
    } else {
      val batch = new RichBatchStatement(batchType, stmts)
      batch.setConsistencyLevel(consistencyLevel)
      batch
    }
  }
}

private[connector] class RoutingBatchStatementBuilder(
    override val batchType: BatchStatement.Type,
    override val consistencyLevel: ConsistencyLevel,
    val routingKeyGenerator: RoutingKeyGenerator
    ) extends BatchStatementBuilder(batchType, consistencyLevel, Some(routingKeyGenerator))

private[connector] class SimpleBatchStatementBuilder(
  override val batchType: BatchStatement.Type,
  override val consistencyLevel: ConsistencyLevel
  ) extends BatchStatementBuilder(batchType, consistencyLevel, None)