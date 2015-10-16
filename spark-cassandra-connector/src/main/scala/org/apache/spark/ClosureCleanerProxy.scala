package org.apache.spark

/**
 * A lightweight wrapper around Spark Context to access the closure cleaner functions.
 */
class ClosureCleanerProxy(sparkContext: SparkContext) extends AnyRef {

  def clean[F <: AnyRef](f: F, checkSerializable: Boolean = true): F = sparkContext.clean(f, checkSerializable)

}
