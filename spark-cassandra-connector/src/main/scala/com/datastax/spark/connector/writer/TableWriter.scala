package com.datastax.spark.connector.writer

import java.io.IOException

import com.datastax.spark.connector.types.{MapType, ListType, ColumnType}
import com.google.common.cache.{CacheLoader, CacheBuilder, LoadingCache}
import org.apache.spark.metrics.OutputMetricsUpdater

import com.datastax.driver.core.BatchStatement.Type
import com.datastax.driver.core._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.util.CountingIterator
import com.datastax.spark.connector.util.Quote._
import org.apache.spark.{Logging, TaskContext}

import scala.collection._
import scala.util.{Success, Failure, Try}


private[connector] case class WriterContext[T](rowWriter: RowWriter[T],
                                               writeConf: WriteConf,
                                               tableDef:TableDef,
                                               columnSelector: IndexedSeq[ColumnRef]) extends Serializable {
  type ColumnSelector = IndexedSeq[ColumnRef]
  val columnNames = rowWriter.columnNames diff writeConf.optionPlaceholders
  val columns:Seq[ColumnDef] = columnNames map(tableDef.columnByName)
  val isCounterUpdate: Boolean = tableDef.columns.exists(_.isCounterColumn)
  val containsCollectionBehaviors: Boolean = columnSelector.exists(_.isInstanceOf[CollectionColumnName])
  val keyspaceName:String = tableDef.keyspaceName
  val tableName = tableDef.tableName
  val batchType = if (isCounterUpdate) Type.COUNTER else Type.UNLOGGED
  lazy val routingKeyGenerator = new RoutingKeyGenerator(tableDef, columnNames)

  def insertQueryTemplate: String = {
    val quotedColumnNames: Seq[String] = columnNames.map(quote)
    val columnSpec = quotedColumnNames.mkString(", ")
    val valueSpec = quotedColumnNames.map(":" + _).mkString(", ")

    val ttlSpec = writeConf.ttl match {
      case TTLOption(PerRowWriteOptionValue(placeholder)) => Some(s"TTL :$placeholder")
      case TTLOption(StaticWriteOptionValue(value)) => Some(s"TTL $value")
      case _ => None
    }

    val timestampSpec = writeConf.timestamp match {
      case TimestampOption(PerRowWriteOptionValue(placeholder)) => Some(s"TIMESTAMP :$placeholder")
      case TimestampOption(StaticWriteOptionValue(value)) => Some(s"TIMESTAMP $value")
      case _ => None
    }

    val options = List(ttlSpec, timestampSpec).flatten
    val optionsSpec = if (options.nonEmpty) s"USING ${options.mkString(" AND ")}" else ""

    s"INSERT INTO ${quote(keyspaceName)}.${quote(tableName)} ($columnSpec) VALUES ($valueSpec) $optionsSpec".trim
  }

  def updateQueryTemplate: String = {
    val (primaryKey, regularColumns) = columns.partition(_.isPrimaryKeyColumn)
    val (counterColumns, nonCounterColumns) = regularColumns.partition(_.isCounterColumn)

    val nameToBehavior = (columnSelector collect {
      case cn:CollectionColumnName => cn.columnName -> cn.collectionBehavior
    }).toMap

    val setNonCounterColumnsClause = for {
    colDef <- nonCounterColumns
    name = colDef.columnName
    collectionBehavior = nameToBehavior.get(name)
    quotedName = quote(name)
    } yield collectionBehavior match {
        case Some(CollectionAppend)           => s"$quotedName = $quotedName + :$quotedName"
        case Some(CollectionPrepend)          => s"$quotedName = :$quotedName + $quotedName"
        case Some(CollectionRemove)           => s"$quotedName = $quotedName - :$quotedName"
        case Some(CollectionOverwrite) | None => s"$quotedName = :$quotedName"
      }

    def quotedColumnNames(columns: Seq[ColumnDef]) = columns.map(_.columnName).map(quote)
    val setCounterColumnsClause = quotedColumnNames(counterColumns).map(c => s"$c = $c + :$c")
    val setClause = (setNonCounterColumnsClause ++ setCounterColumnsClause).mkString(", ")
    val whereClause = quotedColumnNames(primaryKey).map(c => s"$c = :$c").mkString(" AND ")

    s"UPDATE ${quote(keyspaceName)}.${quote(tableName)} SET $setClause WHERE $whereClause"
  }

  lazy val queryTemplate = if (isCounterUpdate || containsCollectionBehaviors) {
    updateQueryTemplate
  } else {
    insertQueryTemplate
  }

  def prepareStatement(session: Session): Try[PreparedStatement] = {
    Try {
      val stmt = session.prepare(queryTemplate)
      stmt.setConsistencyLevel(writeConf.consistencyLevel)
      stmt
    }.recoverWith {
      case t: Throwable =>
        Failure(new IOException(s"Failed to prepare statement $queryTemplate: ${t.getMessage}",t))
    }
  }

  def batchRoutingKey(session: Session): RichBoundStatement => KeyedRichBoundStatement = { bs =>
    def setRoutingKeyIfMissing(bSttmt: BoundStatement): Unit =
      if (bSttmt.getRoutingKey == null) {
        bSttmt.setRoutingKey(routingKeyGenerator(bSttmt))
      }

    val routingKey = writeConf.batchGroupingKey match {
      case BatchGroupingKey.None => 0

      case BatchGroupingKey.ReplicaSet =>
        setRoutingKeyIfMissing(bs)
        session.getCluster.getMetadata.getReplicas(keyspaceName, bs.getRoutingKey).hashCode() // hash code is enough

      case BatchGroupingKey.Partition =>
        setRoutingKeyIfMissing(bs)
        bs.getRoutingKey.duplicate()
    }
    KeyedRichBoundStatement(batchType, routingKey, bs)
  }

  val applyRoutingKey: RichBoundStatement => RichBoundStatement = { stmt =>
    stmt.setRoutingKey(routingKeyGenerator.apply(stmt))
    stmt
  }
}


/** Writes RDD data into given Cassandra table.
  * Individual column values are extracted from RDD objects using given [[RowWriter]]
  * Then, data are inserted into Cassandra with batches of CQL INSERT statements.
  * Each RDD partition is processed by a single thread. */
private[connector] class TableWriter[T](connector: CassandraConnector, val writerCtx: WriterContext[T]) extends Serializable with Logging {


  /** Main entry point */
  def write(taskContext: TaskContext, data: Iterator[T]) {
    val keyspace = writerCtx.keyspaceName
    val tableName = writerCtx.tableName
    val colNames = writerCtx.columnNames
    val updater = OutputMetricsUpdater(taskContext, writerCtx.writeConf)
    connector.withSessionDo { session =>
      val rowIterator = new CountingIterator(data)
      val stmt = writerCtx.prepareStatement(session).get // throws an exception in case the table does not exists

      val queryExecutor = new QueryExecutor(session, writerCtx.writeConf.parallelismLevel,
        Some(updater.batchFinished(success = true, _, _, _)), Some(updater.batchFinished(success = false, _, _, _)))

      val boundStmtBuilder = new BoundStatementBuilder(writerCtx.rowWriter, stmt)
      val batchKeyGenerator = writerCtx.batchRoutingKey(session)
      val keyedRichBoundStatementIterator = rowIterator.map(boundStmtBuilder.bind _ andThen batchKeyGenerator)

      val batcher = new BatcherBuilder(writerCtx.writeConf.consistencyLevel).routingBatcher(writerCtx.routingKeyGenerator)


      val batchBuilder = new GroupingBatchBuilder(batcher, writerCtx.writeConf.batchSize,
        writerCtx.writeConf.batchGroupingBufferSize, keyedRichBoundStatementIterator)

      val rateLimiter = new RateLimiter((writerCtx.writeConf.throughputMiBPS * 1024 * 1024).toLong, 1024 * 1024)

      logDebug(s"Writing data partition to $keyspace.$tableName in batches of ${writerCtx.writeConf.batchSize}.")

      for (stmtToWrite <- batchBuilder) {
        queryExecutor.executeAsync(stmtToWrite)
        assert(stmtToWrite.bytesCount > 0)
        rateLimiter.maybeSleep(stmtToWrite.bytesCount)
      }

      queryExecutor.waitForCurrentlyExecutingTasks()

      if (!queryExecutor.successful)
        throw new IOException(s"Failed to write statements to $keyspace.$tableName.")

      val duration = updater.finish() / 1000000000d
      logInfo(f"Wrote ${rowIterator.count} rows to $keyspace.$tableName in $duration%.3f s.")
    }
  }
}

object TableWriter {

  type Check = Try[Unit]
  def validate[A](p:Boolean)(throwable: () => Throwable): Check = {
    if (p) Failure(throwable()) else Success(())
  }
  def checkAll(checks:Seq[Check]): Check = checks.find(_.isFailure).getOrElse(Success(()))

  private[connector] def checkMissingColumns(table: TableDef, columnNames: Seq[String]): Check = {
    val allColumnNames = table.columns.map(_.columnName)
    val missingColumns = columnNames.toSet -- allColumnNames
    validate(missingColumns.nonEmpty) {
      () => new IllegalArgumentException(s"Column(s) not found: ${missingColumns.mkString(", ")}")
    }
  }

  private[connector] def checkMissingPrimaryKeyColumns(table: TableDef, columnNames: Seq[String]): Check = {
    val primaryKeyColumnNames = table.primaryKey.map(_.columnName)
    val missingPrimaryKeyColumns = primaryKeyColumnNames.toSet -- columnNames
    validate (missingPrimaryKeyColumns.nonEmpty) {
      () => new IllegalArgumentException(
        s"Some primary key columns are missing in RDD or have not been selected: ${missingPrimaryKeyColumns.mkString(", ")}")
    }
  }

  /**
   * Check whether a collection behavior is being applied to a non collection column
   * Check whether prepend is used on any Sets or Maps
   * Check whether remove is used on Maps
   */
  private[connector] def checkCollectionBehaviors(table: TableDef, columnRefs: IndexedSeq[ColumnRef]):Check = {
    val tableCollectionColumns = table.columns.filter(cd => cd.isCollection)
    val tableCollectionColumnNames = tableCollectionColumns.map(_.columnName)
    val tableListColumnNames = tableCollectionColumns
      .map(c => (c.columnName, c.columnType))
      .collect { case (name, x: ListType[_]) => name } // check later

    val tableMapColumnNames = tableCollectionColumns
      .map(c => (c.columnName, c.columnType))
      .collect { case (name, x: MapType[_, _]) => name }

    val refsWithCollectionBehavior = columnRefs collect {
      case columnName: CollectionColumnName => columnName
    }

    val collectionBehaviorColumnNames = refsWithCollectionBehavior.map(_.columnName)

    //Check for non-collection columns with a collection Behavior
    val collectionBehaviorNormalColumn =
      collectionBehaviorColumnNames.toSet -- tableCollectionColumnNames.toSet

    val normalColumnsIllegalBehavior = validate( collectionBehaviorNormalColumn.nonEmpty) {
      () => new IllegalArgumentException(
        s"""Collection behaviors (add/remove/append/prepend) are only allowed on collection columns.
           |Normal Columns with illegal behavior: ${collectionBehaviorNormalColumn.mkString}"""
          .stripMargin
      )
    }

    //Check that prepend is used only on lists
    val prependBehaviorColumnNames = refsWithCollectionBehavior
      .filter(_.collectionBehavior == CollectionPrepend)
      .map(_.columnName)
    val prependOnNonList = prependBehaviorColumnNames.toSet -- tableListColumnNames.toSet

    val prependOnlyToLists = validate (prependOnNonList.nonEmpty) {
      () => new IllegalArgumentException(
        s"""The prepend collection behavior only applies to Lists. Prepend used on:
           |${prependOnNonList.mkString}""".stripMargin)
    }

    //Check that remove is not used on Maps

    val removeBehaviorColumnNames = refsWithCollectionBehavior
      .filter(_.collectionBehavior == CollectionRemove)
      .map(_.columnName)

    val removeOnMap = removeBehaviorColumnNames.toSet & tableMapColumnNames.toSet

    val removeOnlyFromMaps = validate(removeOnMap.nonEmpty) {
      () => new IllegalArgumentException(
        s"The remove operation is currently not supported for Maps. Remove used on: ${removeOnMap.mkString}")
    }
    checkAll(Seq(normalColumnsIllegalBehavior, prependOnlyToLists, removeOnlyFromMaps))
  }

  private[connector] def checkColumns(table: TableDef, columnRefs: IndexedSeq[ColumnRef]): Check = {
    val columnNames = columnRefs.map(_.columnName)
    checkAll(Seq(
      checkMissingColumns(table, columnNames),
      checkMissingPrimaryKeyColumns(table, columnNames),
      checkCollectionBehaviors(table, columnRefs)
    ))
  }

  def functionalWriter[T,U : RowWriterFactory](
      connector: CassandraConnector,
      keyspaceFunc: T => String,
      tableFunc: T=> String,
      dataFunc: T=> U,
      columnNames: ColumnSelector,
      writeConf: WriteConf): DataDependentWriter[T,U] = {

    val rowWriterFactory = implicitly[RowWriterFactory[U]]
    new DataDependentWriter[T,U] (connector, writeConf: WriteConf, rowWriterFactory,
                                  columnNames: ColumnSelector, keyspaceFunc, tableFunc, dataFunc)
  }

  def apply[T : RowWriterFactory](
                                   connector: CassandraConnector,
                                   keyspaceName: String,
                                   tableName: String,
                                   columnNames: ColumnSelector,
                                   writeConf: WriteConf): TableWriter[T] = {

    val schema = Schema.fromCassandra(connector, Some(keyspaceName), Some(tableName))
    val tableDef = schema.tables.headOption
      .getOrElse(throw new IOException(s"Table not found: $keyspaceName.$tableName"))
    val selectedColumns = columnNames.selectFrom(tableDef)
    val optionColumns = writeConf.optionsAsColumns(keyspaceName, tableName)
    val rowWriter = implicitly[RowWriterFactory[T]].rowWriter(
      tableDef.copy(regularColumns = tableDef.regularColumns ++ optionColumns),
      selectedColumns ++ optionColumns.map(_.ref))

    checkColumns(tableDef, selectedColumns).get //trigger rising an exception if any

    val ctx = WriterContext[T](rowWriter, writeConf, tableDef, selectedColumns)
    new TableWriter[T](connector, ctx)
  }

}

case class WriteStats(keyspace:String, table:String, records:Long)
private[connector] class DataDependentWriter[T,U] (connector: CassandraConnector, writeConf: WriteConf, rowWriterFactory: RowWriterFactory[U],
                                                   columnNames: ColumnSelector, keyspaceFunc: T => String, tableFunc: T=> String, dataFunc: T=>U
                                                    ) extends Serializable with Logging {

  val WriterExpirationInMs = 10000 // this needs a configuration

  def write(taskContext: TaskContext, data: Iterator[T]): Seq[WriteStats] = {
    type KeyspaceTable = (String, String)
    // we instantiate a local cache on each executor
    val contextCache: LoadingCache[KeyspaceTable, Try[WriterContext[U]]] = CacheBuilder.newBuilder()
      .expireAfterWrite(WriterExpirationInMs, java.util.concurrent.TimeUnit.MILLISECONDS) // this needs a configuration
      .build(
        new CacheLoader[KeyspaceTable, Try[WriterContext[U]]]() {
          def load(keyspaceTable: KeyspaceTable): Try[WriterContext[U]] = {
            val (keyspace, table) = keyspaceTable
            val schema = Schema.fromCassandra(connector, Some(keyspace), Some(table))
            val tableDefNotFoundFailure: Try[TableDef] = Failure(new IOException(s"Table not found: $keyspace.$table"))
            val tableDef = schema.tables.headOption.fold(tableDefNotFoundFailure)(Success(_))
            val optionColumns = writeConf.optionsAsColumns(keyspace, table)
            val selectedColumns = tableDef.map(t => columnNames.selectFrom(t))

            for {t <- tableDef
                 cols <- selectedColumns
                 _ <- TableWriter.checkColumns(t, cols)
            } yield {
              val rowWriter = rowWriterFactory.rowWriter(
                t.copy(regularColumns = t.regularColumns ++ optionColumns),
                cols ++ optionColumns.map(_.ref))
              WriterContext[U](rowWriter, writeConf, t, cols)
            }
          }
        }
      )


    val rowIterator = new CountingIterator(data)
    val updater = OutputMetricsUpdater(taskContext, writeConf)
    connector.withSessionDo { session =>
      val queryExecutor = new QueryExecutor(session, writeConf.parallelismLevel,
        Some(updater.batchFinished(success = true, _, _, _)), Some(updater.batchFinished(success = false, _, _, _)))
      val maybeSttmtIterator = rowIterator.map { elem =>
        val keyspace = keyspaceFunc(elem)
        val table = tableFunc(elem)
        val data = dataFunc(elem)
        val ctxWrap = contextCache.get((keyspace, table))
        val maybeSttmt = for {ctx <- ctxWrap
                              stmt <- ctx.prepareStatement(session)
                              boundStmtBuilder = new BoundStatementBuilder(ctx.rowWriter, stmt)
                              batchKeyGenerator = ctx.batchRoutingKey(session)

        } yield  (boundStmtBuilder.bind _ andThen ctx.applyRoutingKey andThen batchKeyGenerator)(data)
        maybeSttmt
      }
      val stmtIterator = maybeSttmtIterator.collect{case Success(e) => e}
      val batcher = new BatcherBuilder(writeConf.consistencyLevel).simpleBatcher

      val batchBuilder = new GroupingBatchBuilder(batcher, writeConf.batchSize, writeConf.batchGroupingBufferSize,stmtIterator)
      val rateLimiter = new RateLimiter((writeConf.throughputMiBPS * 1024 * 1024).toLong, 1024 * 1024)

      for (stmtToWrite <- batchBuilder) {
          queryExecutor.executeAsync(stmtToWrite)
          assert(stmtToWrite.bytesCount > 0)
          rateLimiter.maybeSleep(stmtToWrite.bytesCount)
      }

      queryExecutor.waitForCurrentlyExecutingTasks()
    }

    Seq()
  }
}

