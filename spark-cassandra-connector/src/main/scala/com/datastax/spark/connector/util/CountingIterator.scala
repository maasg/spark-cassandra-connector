package com.datastax.spark.connector.util

/** Counts elements fetched form the underlying iterator. Limit causes iterator to terminate early */
class CountingIterator[T](iterator: Iterator[T], limit: Option[Long] = None) extends Iterator[T] {
  private var _count = 0

  val limitCondition: () => Boolean = {
    if (limit.isDefined) {
      val limitValue = limit.get
      () => _count < limitValue && iterator.hasNext
    } else {
      () => iterator.hasNext
    }
  }

  /** Returns the number of successful invocations of `next` */
  def count = _count

  override def hasNext = limitCondition()

  override def next() = {
    val item = iterator.next()
    _count += 1
    item
  }
}
