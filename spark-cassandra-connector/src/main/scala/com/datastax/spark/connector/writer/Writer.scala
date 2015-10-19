package com.datastax.spark.connector.writer

import com.datastax.driver.core.BatchStatement


package object writer {
  type Batcher = BatchStatement.Type => Seq[RichBoundStatement] => RichStatement
}
