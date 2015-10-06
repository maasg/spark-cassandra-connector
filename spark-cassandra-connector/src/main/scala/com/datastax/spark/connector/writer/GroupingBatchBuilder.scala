package com.datastax.spark.connector.writer

import com.datastax.driver.core._
import com.datastax.spark.connector.BatchSize
import com.datastax.spark.connector.util.PriorityHashMap
import com.google.common.collect.AbstractIterator

import scala.annotation.tailrec
import scala.collection.Iterator

/**
 * A grouping batch builder is an iterator which take an iterator of single data items and tries to group
 * those items into batches. For each data item, a batch key is computed with the provided function.
 * The items for which the batch key is the same, are grouped together into a batch.
 *
 * When the batch key for the consecutive data items is different, the items are added to separate
 * batches, and those batches are added to the queue. The queue length is limited, therefore when it is
 * full, the longest batch is removed and returned by the iterator.
 * A batch is removed from the queue also in the case when it reaches the batch size limit.
 *
 * The implementation is based on `PriorityHashMap`.
 *
 * @param batchStatementBuilder a configured batch statement builder
 * @param batchSize             maximum batch size
 * @param maxBatches            maximum number of batches which can remain in the buffer
 * @param data                  data iterator
 */
private[connector] class GroupingBatchBuilder(
    batchStatementBuilder: BatchStatementBuilder,
    batchSize: BatchSize,
    maxBatches: Int,
    data: Iterator[KeyedRichBoundStatement]) extends Iterator[RichStatement] {

  require(maxBatches > 0, "The maximum number of batches must be greater than 0")

  private[this] val batchMap = new PriorityHashMap[Any, Batch](maxBatches)

  /** The method processes the given statement - it adds it to the existing batch or to the new one.
    * If adding the statement would not fit into an existing batch or the new batch would not fit into
    * the buffer, the batch statement is created from the batch and it is returned and the given
    * bound statement is added to a fresh batch. */
  private def processStatement(stmt: KeyedRichBoundStatement): Option[RichStatement] = {
    batchMap.get(stmt.key) match {
      case Some(batch) =>
        updateBatchInMap(batch, stmt)
      case None =>
        addBatchToMap(stmt)
    }
  }

  /** Adds the given statement to the batch if possible; If there is no enough capacity in the batch,
    * a batch statement is created and returned; the batch is cleaned and the given statement is added
    * to it. */
  private def updateBatchInMap(batch: Batch, stmt: KeyedRichBoundStatement): Option[RichStatement] = {
    if (batch.add(stmt.richBoundStatement, force = false)) {
      batchMap.put(stmt.key, batch)
      None
    } else {
      Some(replaceBatch(batch, stmt))
    }
  }

  /** Adds a new batch to the buffer and adds the given statement to it. Returns a statement which had
    * to be dequeued. */
  private def addBatchToMap(stmt: KeyedRichBoundStatement): Option[RichStatement] = {
    if (batchMap.size == maxBatches) {
      Some(replaceBatch(batchMap.dequeue(), stmt))
    } else {
      val batch = Batch(batchSize)
      batch.add(stmt.richBoundStatement, force = true)
      batchMap.put(stmt.key, batch)
      None
    }
  }

  /** Creates a statement from the given batch and cleans the batch so that it can be reused. */
  @inline
  final private def createStmtAndReleaseBatch(batch: Batch): RichStatement = {
    val stmt = batchStatementBuilder.maybeCreateBatch(batch.statements)
    batch.clear()
    stmt
  }

  /** Creates a statement from the given batch; cleans the batch and adds a given statement to it;
    * updates the entry in the buffer. */
  @inline
  private def replaceBatch(batch: Batch, newStatement: KeyedRichBoundStatement): RichStatement = {
    val stmt = createStmtAndReleaseBatch(batch)
    batch.add(newStatement.richBoundStatement, force = true)
    batchMap.put(newStatement.key, batch)
    stmt
  }

  final override def hasNext: Boolean =
    data.hasNext || batchMap.nonEmpty

  @tailrec
  final override def next(): RichStatement = {
    if (data.hasNext) {

      processStatement(data.next()) match {
        case Some(batchStmt) => batchStmt
        case _ => next()
      }

    } else if (batchMap.nonEmpty) {
      createStmtAndReleaseBatch(batchMap.dequeue())
    } else {
      throw new NoSuchElementException("Called next() on empty iterator")
    }
  }

}
