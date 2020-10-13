/*
 * Copyright (2020) The Hyperspace Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.microsoft.hyperspace.actions

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

import com.microsoft.hyperspace.HyperspaceException
import com.microsoft.hyperspace.index._
import com.microsoft.hyperspace.index.DataFrameWriterExtensions.Bucketizer
import com.microsoft.hyperspace.index.IndexConstants.OPTIMIZE_MODES
import com.microsoft.hyperspace.telemetry.{AppInfo, HyperspaceEvent, OptimizeActionEvent}
import com.microsoft.hyperspace.util.{HyperspaceConf, PathUtils}

/**
 * Optimize Action provides an optimize support for indexes where small index files
 * can be merged into larger ones for better index performance.
 *
 * Algorithm outline:
 * 1. Collect all the currently valid index files.
 * 2. Split files into small and large, based on a threshold.
 * 3. Bucketwise combine smaller files into 1 file per bucket.
 * 4. Update index snapshot to remove small files and keep large files + newly created files.
 *
 * NOTE: This is an index-only operation. It does not look at the current state of
 * the data at all. If the data was changed after index creation, optimize will NOT
 * include the data changes.
 *
 * Available modes:
 * `Quick` mode: This mode allows for fast optimization. Files smaller than a
 * predefined threshold "spark.hyperspace.index.optimize.fileSizeThreshold" will be picked
 * for compaction.
 *
 * `Full` mode: This allows for slow but complete optimization. ALL files are
 * eligible for compaction. Compaction threshold is inifinity.
 *
 * TODO: Optimize can be a no-op if there is only one small file per bucket.
 *  https://github.com/microsoft/hyperspace/issues/204.
 *
 * @param spark SparkSession
 * @param logManager Index LogManager for index being refreshed
 * @param dataManager Index DataManager for index being refreshed
 * @param mode Acceptable optimize modes are `quick` and `full`.
 */
class OptimizeAction(
    spark: SparkSession,
    logManager: IndexLogManager,
    dataManager: IndexDataManager,
    mode: String)
    extends RefreshActionBase(spark, logManager, dataManager) {
  final override def op(): Unit = {
    // Rewrite index from small files
    val numBuckets = previousIndexLogEntry.numBuckets
    val indexDF = spark.read.parquet(smallFiles.map(_.name): _*)

    val repartitionedDf =
      indexDF.repartition(numBuckets, indexConfig.indexedColumns.map(indexDF(_)): _*)

    repartitionedDf.write.saveWithBuckets(
      repartitionedDf,
      indexDataPath.toString,
      logEntry.asInstanceOf[IndexLogEntry].numBuckets,
      indexConfig.indexedColumns)
  }

  override def validate(): Unit = {
    super.validate()

    if (!OPTIMIZE_MODES.exists(_.equalsIgnoreCase(mode))) {
      throw HyperspaceException(s"Unsupported optimize mode '$mode' found.")
    }

    if (smallFiles.isEmpty) {
      throw NoChangesException(
        s"Optimize aborted as no index files smaller than " +
          s"${HyperspaceConf.optimizeFileSizeThreshold(spark)} found.")
    }
  }

  private lazy val (smallFiles, largeFiles): (Seq[FileInfo], Seq[FileInfo]) = {
    if (mode.equalsIgnoreCase(IndexConstants.OPTIMIZE_MODE_QUICK)) {
      val threshold = HyperspaceConf.optimizeFileSizeThreshold(spark)
      previousIndexLogEntry.content.fileInfos.toSeq.partition(_.size < threshold)
    } else {
      (previousIndexLogEntry.content.fileInfos.toSeq, Seq())
    }
  }

  override def logEntry: LogEntry = {
    // Use the `previousLogEntry`. Update content of index files replacing `smallFiles` with newly
    // created files.
    val absolutePath = PathUtils.makeAbsolute(indexDataPath)
    val newContent = Content.fromDirectory(absolutePath)
    if (largeFiles.nonEmpty) {
      val largeFilesDirectory: Directory = {
        val fs = new Path(largeFiles.head.name).getFileSystem(new Configuration)
        val largeFileStatuses =
          largeFiles.map(fileInfo => fs.getFileStatus(new Path(fileInfo.name)))

        Directory.fromLeafFiles(largeFileStatuses)
      }
      val mergedContent = Content(newContent.root.merge(largeFilesDirectory))
      previousIndexLogEntry.copy(content = mergedContent)
    } else {
      previousIndexLogEntry.copy(content = newContent)
    }
  }

  override protected def event(appInfo: AppInfo, message: String): HyperspaceEvent = {
    OptimizeActionEvent(appInfo, logEntry.asInstanceOf[IndexLogEntry], message)
  }
}
