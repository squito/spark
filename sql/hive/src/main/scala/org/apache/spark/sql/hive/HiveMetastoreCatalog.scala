/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive

import scala.util.control.NonFatal

import com.google.common.util.concurrent.Striped
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{QualifiedTableName, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.HiveCaseSensitiveInferenceMode._
import org.apache.spark.sql.types._

/**
 * Legacy catalog for interacting with the Hive metastore.
 *
 * This is still used for things like creating data source tables, but in the future will be
 * cleaned up to integrate more nicely with [[HiveExternalCatalog]].
 */
private[hive] class HiveMetastoreCatalog(sparkSession: SparkSession) extends Logging {
  // these are def_s and not val/lazy val since the latter would introduce circular references
  private def sessionState = sparkSession.sessionState
  private def tableRelationCache = sparkSession.sessionState.catalog.tableRelationCache
  import HiveMetastoreCatalog._

  /** These locks guard against multiple attempts to instantiate a table, which wastes memory. */
  private val tableCreationLocks = Striped.lazyWeakLock(100)

  /** Acquires a lock on the table cache for the duration of `f`. */
  private def withTableCreationLock[A](tableName: QualifiedTableName, f: => A): A = {
    val lock = tableCreationLocks.get(tableName)
    lock.lock()
    try f finally {
      lock.unlock()
    }
  }

  // For testing only
  private[hive] def getCachedDataSourceTable(table: TableIdentifier): LogicalPlan = {
    val key = QualifiedTableName(
      table.database.getOrElse(sessionState.catalog.getCurrentDatabase).toLowerCase,
      table.table.toLowerCase)
    tableRelationCache.getIfPresent(key)
  }

  private def getCached(
      tableIdentifier: QualifiedTableName,
      pathsInMetastore: Seq[Path],
      schemaInMetastore: StructType,
      expectedFileFormat: Class[_ <: FileFormat],
      partitionSchema: Option[StructType]): Option[LogicalRelation] = {

    tableRelationCache.getIfPresent(tableIdentifier) match {
      case null => None // Cache miss
      case logical @ LogicalRelation(relation: HadoopFsRelation, _, _) =>
        val cachedRelationFileFormatClass = relation.fileFormat.getClass

        expectedFileFormat match {
          case `cachedRelationFileFormatClass` =>
            // If we have the same paths, same schema, and same partition spec,
            // we will use the cached relation.
            val useCached =
              relation.location.rootPaths.toSet == pathsInMetastore.toSet &&
                logical.schema.sameType(schemaInMetastore) &&
                // We don't support hive bucketed tables. This function `getCached` is only used for
                // converting supported Hive tables to data source tables.
                relation.bucketSpec.isEmpty &&
                relation.partitionSchema == partitionSchema.getOrElse(StructType(Nil))

            if (useCached) {
              Some(logical)
            } else {
              // If the cached relation is not updated, we invalidate it right away.
              tableRelationCache.invalidate(tableIdentifier)
              None
            }
          case _ =>
            logWarning(s"Table $tableIdentifier should be stored as $expectedFileFormat. " +
              s"However, we are getting a ${relation.fileFormat} from the metastore cache. " +
              "This cached entry will be invalidated.")
            tableRelationCache.invalidate(tableIdentifier)
            None
        }
      case other =>
        logWarning(s"Table $tableIdentifier should be stored as $expectedFileFormat. " +
          s"However, we are getting a $other from the metastore cache. " +
          "This cached entry will be invalidated.")
        tableRelationCache.invalidate(tableIdentifier)
        None
    }
  }

  def convertToLogicalRelation(
      relation: CatalogRelation,
      options: Map[String, String],
      fileFormatClass: Class[_ <: FileFormat],
      fileType: String): LogicalRelation = {
    val metastoreSchema = relation.tableMeta.schema
    val tableIdentifier =
      QualifiedTableName(relation.tableMeta.database, relation.tableMeta.identifier.table)

    val lazyPruningEnabled = sparkSession.sqlContext.conf.manageFilesourcePartitions
    val tablePath = new Path(relation.tableMeta.location)
    val fileFormat = fileFormatClass.newInstance()

    val result = if (relation.isPartitioned) {
      val partitionSchema = relation.tableMeta.partitionSchema
      val rootPaths: Seq[Path] = if (lazyPruningEnabled) {
        Seq(tablePath)
      } else {
        // By convention (for example, see CatalogFileIndex), the definition of a
        // partitioned table's paths depends on whether that table has any actual partitions.
        // Partitioned tables without partitions use the location of the table's base path.
        // Partitioned tables with partitions use the locations of those partitions' data
        // locations,_omitting_ the table's base path.
        val paths = sparkSession.sharedState.externalCatalog
          .listPartitions(tableIdentifier.database, tableIdentifier.name)
          .map(p => new Path(p.storage.locationUri.get))

        if (paths.isEmpty) {
          Seq(tablePath)
        } else {
          paths
        }
      }

      withTableCreationLock(tableIdentifier, {
        val cached = getCached(
          tableIdentifier,
          rootPaths,
          metastoreSchema,
          fileFormatClass,
          Some(partitionSchema))

        val logicalRelation = cached.getOrElse {
          val sizeInBytes = relation.stats(sparkSession.sessionState.conf).sizeInBytes.toLong
          val fileIndex = {
            val index = new CatalogFileIndex(sparkSession, relation.tableMeta, sizeInBytes)
            if (lazyPruningEnabled) {
              index
            } else {
              index.filterPartitions(Nil)  // materialize all the partitions in memory
            }
          }

          val (dataSchema, updatedTable) =
            inferIfNeeded(relation, options, fileFormat, Option(fileIndex))

          val fsRelation = HadoopFsRelation(
            location = fileIndex,
            partitionSchema = partitionSchema,
            dataSchema = dataSchema,
            // We don't support hive bucketed tables, only ones we write out.
            bucketSpec = None,
            fileFormat = fileFormat,
            options = options ++ getStorageTzOptions(relation))(sparkSession = sparkSession)
          val created = LogicalRelation(fsRelation, updatedTable)
          tableRelationCache.put(tableIdentifier, created)
          created
        }

        logicalRelation
      })
    } else {
      val rootPath = tablePath
      withTableCreationLock(tableIdentifier, {
        val cached = getCached(
          tableIdentifier,
          Seq(rootPath),
          metastoreSchema,
          fileFormatClass,
          None)
        val logicalRelation = cached.getOrElse {
          val (dataSchema, updatedTable) = inferIfNeeded(relation, options, fileFormat)
          val created =
            LogicalRelation(
              DataSource(
                sparkSession = sparkSession,
                paths = rootPath.toString :: Nil,
                userSpecifiedSchema = Option(dataSchema),
                // We don't support hive bucketed tables, only ones we write out.
                bucketSpec = None,
                options = options ++ getStorageTzOptions(relation),
                className = fileType).resolveRelation(),
              table = updatedTable)

          tableRelationCache.put(tableIdentifier, created)
          created
        }

        logicalRelation
      })
    }
    // The inferred schema may have different filed names as the table schema, we should respect
    // it, but also respect the exprId in table relation output.
    assert(result.output.length == relation.output.length &&
      result.output.zip(relation.output).forall { case (a1, a2) => a1.dataType == a2.dataType })
    val newOutput = result.output.zip(relation.output).map {
      case (a1, a2) => a1.withExprId(a2.exprId)
    }
    result.copy(output = newOutput)
  }

  private def getStorageTzOptions(relation: CatalogRelation): Map[String, String] = {
    // We add the table timezone to the relation options, which automatically gets injected into the
    // hadoopConf for the Parquet Converters
    val storageTzKey = ParquetFileFormat.PARQUET_TIMEZONE_TABLE_PROPERTY
    relation.tableMeta.properties.get(storageTzKey).map(storageTzKey -> _).toMap
  }

  private def inferIfNeeded(
      relation: CatalogRelation,
      options: Map[String, String],
      fileFormat: FileFormat,
      fileIndexOpt: Option[FileIndex] = None): (StructType, CatalogTable) = {
    val inferenceMode = sparkSession.sessionState.conf.caseSensitiveInferenceMode
    val shouldInfer = (inferenceMode != NEVER_INFER) && !relation.tableMeta.schemaPreservesCase
    val tableName = relation.tableMeta.identifier.unquotedString
    if (shouldInfer) {
      logInfo(s"Inferring case-sensitive schema for table $tableName (inference mode: " +
        s"$inferenceMode)")
      val fileIndex = fileIndexOpt.getOrElse {
        val rootPath = new Path(relation.tableMeta.location)
        new InMemoryFileIndex(sparkSession, Seq(rootPath), options, None)
      }

      val inferredSchema = fileFormat
        .inferSchema(
          sparkSession,
          options,
          fileIndex.listFiles(Nil, Nil).flatMap(_.files))
        .map(mergeWithMetastoreSchema(relation.tableMeta.schema, _))

      inferredSchema match {
        case Some(schema) =>
          if (inferenceMode == INFER_AND_SAVE) {
            updateCatalogSchema(relation.tableMeta.identifier, schema)
          }
          (schema, relation.tableMeta.copy(schema = schema))
        case None =>
          logWarning(s"Unable to infer schema for table $tableName from file format " +
            s"$fileFormat (inference mode: $inferenceMode). Using metastore schema.")
          (relation.tableMeta.schema, relation.tableMeta)
      }
    } else {
      (relation.tableMeta.schema, relation.tableMeta)
    }
  }

  private def updateCatalogSchema(identifier: TableIdentifier, schema: StructType): Unit = try {
    val db = identifier.database.get
    logInfo(s"Saving case-sensitive schema for table ${identifier.unquotedString}")
    sparkSession.sharedState.externalCatalog.alterTableSchema(db, identifier.table, schema)
  } catch {
    case NonFatal(ex) =>
      logWarning(s"Unable to save case-sensitive schema for table ${identifier.unquotedString}", ex)
  }
}


private[hive] object HiveMetastoreCatalog {
  def mergeWithMetastoreSchema(
      metastoreSchema: StructType,
      inferredSchema: StructType): StructType = try {
    // Find any nullable fields in mestastore schema that are missing from the inferred schema.
    val metastoreFields = metastoreSchema.map(f => f.name.toLowerCase -> f).toMap
    val missingNullables = metastoreFields
      .filterKeys(!inferredSchema.map(_.name.toLowerCase).contains(_))
      .values
      .filter(_.nullable)
    // Merge missing nullable fields to inferred schema and build a case-insensitive field map.
    val inferredFields = StructType(inferredSchema ++ missingNullables)
      .map(f => f.name.toLowerCase -> f).toMap
    StructType(metastoreSchema.map(f => f.copy(name = inferredFields(f.name).name)))
  } catch {
    case NonFatal(_) =>
      val msg = s"""Detected conflicting schemas when merging the schema obtained from the Hive
         | Metastore with the one inferred from the file format. Metastore schema:
         |${metastoreSchema.prettyJson}
         |
         |Inferred schema:
         |${inferredSchema.prettyJson}
       """.stripMargin
      throw new SparkException(msg)
  }
}
