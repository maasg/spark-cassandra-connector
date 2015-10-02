package com.datastax.spark.connector.writer

import java.io.IOException

import com.datastax.spark.connector.types.{MapType, ListType, ColumnType}
import org.apache.spark.metrics.OutputMetricsUpdater

import com.datastax.driver.core.BatchStatement.Type
import com.datastax.driver.core._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.util.CountingIterator
import com.datastax.spark.connector.util.Quote._
import org.apache.spark.{Logging, TaskContext}

import scala.collection._

/** Writes RDD data into given Cassandra table.
  * Individual column values are extracted from RDD objects using given [[RowWriter]]
  * Then, data are inserted into Cassandra with batches of CQL INSERT statements.
  * Each RDD partition is processed by a single thread. */
class TableWriter[T] private (
    connector: CassandraConnector,
    tableDef: TableDef,
    columnSelector: IndexedSeq[ColumnRef],
    rowWriter: RowWriter[T],
    writeConf: WriteConf) extends Serializable with Logging {

  type ColumnSelector = IndexedSeq[ColumnRef]

//  val keyspaceName = tableDef.keyspaceName
//  val tableName = tableDef.tableName

  def columns(rowWriter: RowWriter[T], writeConf: WriteConf, tableDef:TableDef):Seq[ColumnDef] =
    columnNames(rowWriter, writeConf).map(tableDef.columnByName)


  private val isCounterUpdate: TableDef => Boolean = tableDef =>
    tableDef.columns.exists(_.isCounterColumn)

  private val containsCollectionBehaviors: ColumnSelector => Boolean = columnSelector =>
    columnSelector.exists(_.isInstanceOf[CollectionColumnName])

  def columnNames(rowWriter: RowWriter[T], writeConf: WriteConf) = rowWriter.columnNames diff writeConf.optionPlaceholders

  private def prepareStatement(session: Session, keyspace:String, tableDef:TableDef, writeConf: WriteConf, rowWriter:RowWriter[T], columnSelector: ColumnSelector ): PreparedStatement = {
    val keyspaceName = tableDef.keyspaceName
    val tableName = tableDef.tableName
    val colNames = columnNames(rowWriter, writeConf)
    val colDef = columns(rowWriter, writeConf, tableDef)
    val queryTemplate = if (isCounterUpdate(tableDef) || containsCollectionBehaviors(columnSelector)) {
      TableWriter.mkUpdateQueryTemplate(keyspaceName,tableName,colDef, columnSelector)
    } else {
      TableWriter.mkInsertQueryTemplate(keyspaceName, tableName, colNames, writeConf)
    }

    try {
      session.prepare(queryTemplate)
    }
    catch {
      case t: Throwable =>
        throw new IOException(s"Failed to prepare statement $queryTemplate: ${t.getMessage}", t)
    }
  }

  def batchRoutingKey(session: Session, keyspaceName:String, routingKeyGenerator: RoutingKeyGenerator,
                      writeConf:WriteConf)(bs: BoundStatement): Any = {

    def setRoutingKeyIfMissing(bSttmt: BoundStatement): Unit =
      if (bSttmt.getRoutingKey == null) {
        bSttmt.setRoutingKey(routingKeyGenerator(bSttmt))
      }

    writeConf.batchGroupingKey match {
      case BatchGroupingKey.None => 0

      case BatchGroupingKey.ReplicaSet =>
        setRoutingKeyIfMissing(bs)
        session.getCluster.getMetadata.getReplicas(keyspaceName, bs.getRoutingKey).hashCode() // hash code is enough

      case BatchGroupingKey.Partition =>
        setRoutingKeyIfMissing(bs)
        bs.getRoutingKey.duplicate()
    }
  }

  /** Main entry point */
  def write(taskContext: TaskContext, data: Iterator[T]) {
    val keyspace = tableDef.keyspaceName
    val tableName = tableDef.tableName
    val colNames = columnNames(rowWriter, writeConf)
    val updater = OutputMetricsUpdater(taskContext, writeConf)
    connector.withSessionDo { session =>
      val rowIterator = new CountingIterator(data)
      val stmt = prepareStatement(session, keyspace, tableDef, writeConf, rowWriter, columnSelector)
        stmt.setConsistencyLevel(writeConf.consistencyLevel)
      val queryExecutor = new QueryExecutor(session, writeConf.parallelismLevel,
        Some(updater.batchFinished(success = true, _, _, _)), Some(updater.batchFinished(success = false, _, _, _)))
      val routingKeyGenerator = new RoutingKeyGenerator(tableDef, colNames)
      val batchType = if (isCounterUpdate(tableDef)) Type.COUNTER else Type.UNLOGGED
      val boundStmtBuilder = new BoundStatementBuilder(rowWriter, stmt)
      val batchStmtBuilder = new BatchStatementBuilder(batchType, routingKeyGenerator, writeConf.consistencyLevel)
      val batchKeyGenerator = batchRoutingKey(session, keyspace, routingKeyGenerator, writeConf) _
      val batchBuilder = new GroupingBatchBuilder(boundStmtBuilder, batchStmtBuilder, batchKeyGenerator,
        writeConf.batchSize, writeConf.batchGroupingBufferSize, rowIterator)
      val rateLimiter = new RateLimiter((writeConf.throughputMiBPS * 1024 * 1024).toLong, 1024 * 1024)

      logDebug(s"Writing data partition to $keyspace.$tableName in batches of ${writeConf.batchSize}.")

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

  private[connector] def mkInsertQueryTemplate(keyspaceName: String, tableName:String,
                                                    columnNames:Seq[String], writeConf: WriteConf): String = {
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

  private[connector] def mkUpdateQueryTemplate(keyspaceName:String, tableName: String, columns: Seq[ColumnDef],
                                                    columnSelector: IndexedSeq[ColumnRef]): String = {
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

  private def checkMissingColumns(table: TableDef, columnNames: Seq[String]) {
    val allColumnNames = table.columns.map(_.columnName)
    val missingColumns = columnNames.toSet -- allColumnNames
    if (missingColumns.nonEmpty)
      throw new IllegalArgumentException(
        s"Column(s) not found: ${missingColumns.mkString(", ")}")
  }

  private def checkMissingPrimaryKeyColumns(table: TableDef, columnNames: Seq[String]) {
    val primaryKeyColumnNames = table.primaryKey.map(_.columnName)
    val missingPrimaryKeyColumns = primaryKeyColumnNames.toSet -- columnNames
    if (missingPrimaryKeyColumns.nonEmpty)
      throw new IllegalArgumentException(
        s"Some primary key columns are missing in RDD or have not been selected: ${missingPrimaryKeyColumns.mkString(", ")}")
  }

  /**
   * Check whether a collection behavior is being applied to a non collection column
   * Check whether prepend is used on any Sets or Maps
   * Check whether remove is used on Maps
   */
  private def checkCollectionBehaviors(table: TableDef, columnRefs: IndexedSeq[ColumnRef]) {
    val tableCollectionColumns = table.columns.filter(cd => cd.isCollection)
    val tableCollectionColumnNames = tableCollectionColumns.map(_.columnName)
    val tableListColumnNames = tableCollectionColumns
      .map(c => (c.columnName, c.columnType))
      .collect { case (name, x: ListType[_]) => name }

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

    if (collectionBehaviorNormalColumn.nonEmpty)
      throw new IllegalArgumentException(
        s"""Collection behaviors (add/remove/append/prepend) are only allowed on collection columns.
           |Normal Columns with illegal behavior: ${collectionBehaviorNormalColumn.mkString}"""
          .stripMargin
      )

    //Check that prepend is used only on lists
    val prependBehaviorColumnNames = refsWithCollectionBehavior
      .filter(_.collectionBehavior == CollectionPrepend)
      .map(_.columnName)
    val prependOnNonList = prependBehaviorColumnNames.toSet -- tableListColumnNames.toSet

    if (prependOnNonList.nonEmpty)
      throw new IllegalArgumentException(
        s"""The prepend collection behavior only applies to Lists. Prepend used on:
           |${prependOnNonList.mkString}""".stripMargin
      )

    //Check that remove is not used on Maps

    val removeBehaviorColumnNames = refsWithCollectionBehavior
      .filter(_.collectionBehavior == CollectionRemove)
      .map(_.columnName)

    val removeOnMap = removeBehaviorColumnNames.toSet & tableMapColumnNames.toSet

    if (removeOnMap.nonEmpty)
      throw new IllegalArgumentException(
        s"The remove operation is currently not supported for Maps. Remove used on: ${removeOnMap
          .mkString}"
      )
  }

  private def checkColumns(table: TableDef, columnRefs: IndexedSeq[ColumnRef]) = {
    val columnNames = columnRefs.map(_.columnName)
    checkMissingColumns(table, columnNames)
    checkMissingPrimaryKeyColumns(table, columnNames)
    checkCollectionBehaviors(table, columnRefs)
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

    checkColumns(tableDef, selectedColumns)
    new TableWriter[T](connector, tableDef, selectedColumns, rowWriter, writeConf)
  }
}
