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
  /*
  override def logEntry: LogEntry = {
    val absolutePath = PathUtils.makeAbsolute(indexDataPath)
    val newContent = Directory.fromDirectory(absolutePath, fileIdTracker)

    val entry = previousIndexLogEntry
    val mergedIndexContent = Content(newContent.merge(entry.content.root))
    val relation = entry.relations.head
    val originalData = relation.data.properties.content.root
    val newlyAddedData =
      Directory.fromDirectory(PathUtils.makeAbsolute("glob2/y=2023"), fileIdTracker)
    val mergedDataContent = Content(originalData.merge(newlyAddedData))

    // This is required to correctly recalculate the signature.
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

    // Signature
    val signatureProvider = LogicalPlanSignatureProvider.create()
    val signature = signatureProvider.signature(innerDf.queryExecution.optimizedPlan) match {
      case Some(s) =>
        LogicalPlanFingerprint(
          LogicalPlanFingerprint.Properties(Seq(Signature(signatureProvider.name, s))))

      case None => throw HyperspaceException("Invalid plan for creating an index.")
    }

    entry.copy(
      content = mergedIndexContent,
      source = entry.source.copy(
        plan = entry.source.plan.copy(
          properties = entry.source.plan.properties.copy(
            relations = Seq(relation.copy(
              data = relation.data.copy(
                properties = relation.data.properties.copy(mergedDataContent)))),
            fingerprint = signature))
          ))
  }
   */

  override def logEntry: LogEntry = {
    val relation = previousIndexLogEntry.relations.head
    val originalData = relation.data.properties.content.root
    val newlyAddedData =
      Directory.fromDirectory(PathUtils.makeAbsolute("glob2/y=2023"), fileIdTracker)
    val mergedDataContent = Content(originalData.merge(newlyAddedData))

    // This is required to correctly recalculate the signature in generated index log entry.
    val innerDf = {
      val relation = previousIndexLogEntry.relations.head
      val dataSchema = DataType.fromJson(relation.dataSchemaJson).asInstanceOf[StructType]
      val paths: Seq[String] = mergedDataContent.files.map(_.toString)
      val basePath = "glob2"
      spark.read
        .schema(dataSchema)
        .format(relation.fileFormat)
        .options(relation.options)
        .option("basePath", basePath)
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
    val pathOption = "glob2/y=2023"
    val basePath = "glob2"
    val dataSchema = DataType.fromJson(relation.dataSchemaJson).asInstanceOf[StructType]
    spark.read
      .schema(dataSchema)
      .format(relation.fileFormat)
      .options(relation.options)
      .option("basePath", basePath)
      .load(pathOption)
  }

  override lazy val deletedFiles: Seq[FileInfo] = {
    // TODO: originalFiles.scanPattern -- current files
    Seq()
  }
}
