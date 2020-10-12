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

package com.microsoft.hyperspace

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import com.microsoft.hyperspace.MockEventLogger.reset
import com.microsoft.hyperspace.index.IndexLogEntry
import com.microsoft.hyperspace.telemetry.{EventLogger, HyperspaceEvent}
import com.microsoft.hyperspace.util.FileUtils

object TestUtils {
  def copyWithState(index: IndexLogEntry, state: String): IndexLogEntry = {
    val result = index.copy()
    result.state = state
    result
  }

  /**
   * Split path into its segments and return segment names as a sequence.
   * For e.g. a path "file:/C:/d1/d2/d3/f1.parquet" will return
   * Seq("f1.parquet", "d3", "d2", "d1", "file:/C:/")
   *
   * @param path Path to split into segments.
   * @return Segments as a seq.
   */
  def splitPath(path: Path): Seq[String] = {
    if (path.getParent == null) {
      // `path` is now root. It's getName returns "" but toString returns actual path,
      // E.g. "file:/C:/" for Windows.
      Seq(path.toString)
    } else {
      path.getName +: splitPath(path.getParent)
    }
  }

  /**
   * Delete some files from a given path.
   *
   * @param path Path to the parent folder containing data files.
   * @param isPartitioned Is data folder partitioned or not.
   * @param pattern File name pattern to delete.
   * @param numFilesToDelete Num files to delete.
   * @return Paths to the deleted file.
   */
  def deleteDataFiles(
      path: String,
      isPartitioned: Boolean = false,
      pattern: String = "*parquet",
      numFilesToDelete: Int = 1): Seq[Path] = {
    val dataPath = if (isPartitioned) {
      new Path(s"$path/*/*", pattern)
    } else {
      new Path(path, pattern)
    }

    val dataFileNames = dataPath
      .getFileSystem(new Configuration)
      .globStatus(dataPath)
      .map(_.getPath)

    assert(dataFileNames.length >= numFilesToDelete)
    val filesToDelete = dataFileNames.take(numFilesToDelete)
    filesToDelete.foreach(FileUtils.delete(_))

    filesToDelete
  }
}

/**
 * This class can be used to test emitted events from Hyperspace actions.
 */
class MockEventLogger extends EventLogger {
  import com.microsoft.hyperspace.MockEventLogger.emittedEvents
  // Reset events.
  reset()

  override def logEvent(event: HyperspaceEvent): Unit = {
    emittedEvents :+= event
  }
}

object MockEventLogger {
  var emittedEvents: Seq[HyperspaceEvent] = Seq()

  def reset(): Unit = {
    emittedEvents = Seq()
  }
}
