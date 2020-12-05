package com.microsoft.hyperspace.actions
// scalastyle:off

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation, PartitioningAwareFileIndex}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import com.microsoft.hyperspace.{Hyperspace, HyperspaceException}
import com.microsoft.hyperspace.index._
import com.microsoft.hyperspace.telemetry.{AppInfo, HyperspaceEvent}
import com.microsoft.hyperspace.util.PathUtils

class RefreshScan(
    spark: SparkSession,
    logManager: IndexLogManager,
    dataManager: IndexDataManager,
    scanPattern: Option[String] = None)
    extends RefreshIncrementalAction(spark, logManager, dataManager) {

  /** df representing the complete set of data files which will be indexed once this refresh action
   * finishes. */
  override protected lazy val df = {
    val relation = previousIndexLogEntry.relations.head
    val previouslyIndexedData = relation.data.properties.content
    val newlyIndexedData = previouslyIndexedData.fileInfos -- deletedFiles ++ appendedFiles
    val newlyIndexedDataFiles: Seq[String] = newlyIndexedData.map(_.name).toSeq

    val dataSchema = DataType.fromJson(relation.dataSchemaJson).asInstanceOf[StructType]
    spark.read
      .schema(dataSchema)
      .format(relation.fileFormat)
      .options(relation.options)
      .load(newlyIndexedDataFiles: _*)
  }

  /** paths resolved with scan pattern */
  private lazy val resolvedPaths = {
    val relation = previousIndexLogEntry.relations.head
    relation.rootPaths.filter(p => matchesScanPattern(p))
    Seq("glob2/y=2023")
  }

  private def matchesScanPattern(path: String): Boolean = {
    // TODO: implement matching logic with scanPattern
    true
  }

  override def logEntry: LogEntry = {
    val entry = getIndexLogEntry(spark, df, indexConfig, indexDataPath)

    // If there is no deleted files, there are index data files only for appended data in this
    // version and we need to add the index data files of previous index version.
    // Otherwise, as previous index data is rewritten in this version while excluding
    // indexed rows from deleted files, all necessary index data files exist in this version.
    val updatedEntry = if (deletedFiles.isEmpty) {
      // Merge new index files with old index files.
      val mergedContent = Content(previousIndexLogEntry.content.root.merge(entry.content.root))
      entry.copy(content = mergedContent)
    } else {
      // New entry.
      entry
    }

    val relation = entry.source.plan.properties.relations.head
    val updatedRelation = relation.copy(rootPaths = previousIndexLogEntry.relations.head.rootPaths)
    updatedEntry.copy(
      source = updatedEntry.source.copy(plan = updatedEntry.source.plan.copy(properties =
        updatedEntry.source.plan.properties.copy(relations = Seq(updatedRelation)))))
  }

  /** deleted files which match scan pattern */
  override protected lazy val deletedFiles: Seq[FileInfo] = {
    val relation = previousIndexLogEntry.relations.head
    val originalFiles =
      relation.data.properties.content.fileInfos.filter(f => matchesScanPattern(f.name))

    (originalFiles -- currentFiles).toSeq

    // TODO: remove this after scan pattern resolution is done
    Seq()
  }

  override protected lazy val currentFiles: Set[FileInfo] = {
    val relation = previousIndexLogEntry.relations.head
    val dataSchema = DataType.fromJson(relation.dataSchemaJson).asInstanceOf[StructType]
    val changedDf = spark.read
      .schema(dataSchema)
      .format(relation.fileFormat)
      .options(relation.options)
      .load(resolvedPaths: _*)
    changedDf.queryExecution.optimizedPlan
      .collect {
        case LogicalRelation(
            HadoopFsRelation(location: PartitioningAwareFileIndex, _, _, _, _, _),
            _,
            _,
            _) =>
          location
            .allFiles()
            .map { f =>
              // For each file, if it already has a file id, add that id to its corresponding
              // FileInfo. Note that if content of an existing file is changed, it is treated
              // as a new file (i.e. its current file id is no longer valid).
              val id = fileIdTracker.addFile(f)
              FileInfo(f, id, asFullPath = true)
            }
      }
      .flatten
      .toSet
  }
}
