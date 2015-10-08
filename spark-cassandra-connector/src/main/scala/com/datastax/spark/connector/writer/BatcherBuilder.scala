package com.datastax.spark.connector.writer

import com.datastax.driver.core._
import com.datastax.spark.connector.writer.writer.Batcher
import org.apache.spark.Logging

private[connector] class BatcherBuilder(val consistencyLevel: ConsistencyLevel) extends Logging {

  private def validate(stmts: Seq[RichBoundStatement]) = require(stmts.size > 0, "Statements list cannot be empty")

  /** Converts a sequence of statements into a batch of the given type if its size is greater than 1
   * @return a function that can batch sequences of RichStatements given a batch type
   */
  def simpleBatcher: Batcher = batchType =>  stmts => {
    validate(stmts)
    if (stmts.size == 1) {
      val sttmt = stmts.head
      sttmt.setConsistencyLevel(consistencyLevel)
      sttmt
    } else {
      val batch = new RichBatchStatement(batchType, stmts)
      batch.setConsistencyLevel(consistencyLevel)
      batch
    }
  }

  /** Converts a sequence of statements into a batch of the given type if its size is greater than 1.
    * Sets the routing key and consistency level.
    * @param routingKeyGenerator  a routingKeyGenerator for the elements of the batch
    * @return a function that can batch sequences of RichStatements given a batch type
    */
  def routingBatcher(routingKeyGenerator: RoutingKeyGenerator): Batcher = batchType => stmts => {
    validate(stmts)
    stmts.head.setRoutingKey(routingKeyGenerator.apply(stmts.head))
    simpleBatcher(batchType)(stmts)
  }

}
