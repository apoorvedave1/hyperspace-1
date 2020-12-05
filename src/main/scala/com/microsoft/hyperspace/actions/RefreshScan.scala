package com.microsoft.hyperspace.actions
// scalastyle:off

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import com.microsoft.hyperspace.HyperspaceException
import com.microsoft.hyperspace.index._
import com.microsoft.hyperspace.telemetry.{AppInfo, HyperspaceEvent}
import com.microsoft.hyperspace.util.PathUtils

class RefreshScan(
    spark: SparkSession,
    logManager: IndexLogManager,
    dataManager: IndexDataManager,
    scanPattern: Option[String] = None)
    extends RefreshIncrementalAction(spark, logManager, dataManager) {
  override def logEntry: LogEntry = {

    // deletedFiles = deleted files which satisfy scan pattern
    // appendedFiles = appendedFiles which satisfy scan pattern

    // previously indexed data files
    // previously created index files

    // now: indexed data files = previously indexed data files ++ newly appended files -- deleted files
    // now: index files = previous index files --

    val relation = previousIndexLogEntry.relations.head
    val previouslyIndexedData = relation.data.properties.content.root
    val newlyAddedData =
      Directory.fromDirectory(PathUtils.makeAbsolute("glob2/y=2023"), fileIdTracker)
    val mergedDataContent = Content(previouslyIndexedData.merge(newlyAddedData))

    // This is required to correctly recalculate the signature in generated index log entry.
    val innerDf = {
      val relation = previousIndexLogEntry.relations.head
      val dataSchema = DataType.fromJson(relation.dataSchemaJson).asInstanceOf[StructType]
      val paths: Seq[String] = mergedDataContent.files.map(_.toString)
      spark.read
        .schema(dataSchema)
        .format(relation.fileFormat)
        .options(relation.options)
        .load(paths: _*)
    }

    val entry = getIndexLogEntry(spark, innerDf, indexConfig, indexDataPath)

    // If there is no deleted files, there are index data files only for appended data in this
    // version and we need to add the index data files of previous index version.
    // Otherwise, as previous index data is rewritten in this version while excluding
    // indexed rows from deleted files, all necessary index data files exist in this version.
    if (deletedFiles.isEmpty) {
      // Merge new index files with old index files.
      val mergedContent = Content(previousIndexLogEntry.content.root.merge(entry.content.root))
      entry.copy(content = mergedContent)
    } else {
      // New entry.
      entry
    }
  }

  override protected lazy val df = {
    val relation = previousIndexLogEntry.relations.head
    val dataSchema = DataType.fromJson(relation.dataSchemaJson).asInstanceOf[StructType]
    spark.read
      .schema(dataSchema)
      .format(relation.fileFormat)
      .options(relation.options)
      .load(resolvedPaths: _*)
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

  override lazy val deletedFiles: Seq[FileInfo] = {
    val relation = previousIndexLogEntry.relations.head
    val originalFiles =
     relation.data.properties.content.fileInfos.filter(f => matchesScanPattern(f.name))

    (originalFiles -- currentFiles).toSeq

    // TODO: remove this after scan pattern resolution is done
    Seq()
  }
}
