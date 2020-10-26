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

package com.microsoft.hyperspace.util

import org.apache.spark.sql.SparkSession

import com.microsoft.hyperspace.index.{Content, CoveringIndex, IndexConstants, IndexLogEntry, LogEntry, Source}

/**
 * Helper class to extract Hyperspace-related configs from SparkSession.
 */
object HyperspaceConf {
  def hybridScanEnabled(spark: SparkSession): Boolean = {
    spark.conf
      .get(
        IndexConstants.INDEX_HYBRID_SCAN_ENABLED,
        IndexConstants.INDEX_HYBRID_SCAN_ENABLED_DEFAULT)
      .toBoolean
  }

  def hybridScanDeleteEnabled(spark: SparkSession): Boolean = {
    spark.conf
      .get(
        IndexConstants.INDEX_HYBRID_SCAN_DELETE_ENABLED,
        IndexConstants.INDEX_HYBRID_SCAN_DELETE_ENABLED_DEFAULT)
      .toBoolean
  }

  def optimizeFileSizeThreshold(spark: SparkSession): Long = {
    spark.conf
      .get(
        IndexConstants.OPTIMIZE_FILE_SIZE_THRESHOLD,
        IndexConstants.OPTIMIZE_FILE_SIZE_THRESHOLD_DEFAULT.toString)
      .toLong
  }

  def hybridScanDeleteMaxNumFiles(spark: SparkSession): Int = {
    spark.conf
      .get(
        IndexConstants.INDEX_HYBRID_SCAN_DELETE_MAX_NUM_FILES,
        IndexConstants.INDEX_HYBRID_SCAN_DELETE_MAX_NUM_FILES_DEFAULT)
      .toInt
  }

  def numBucketsForIndex(spark: SparkSession): Int = {
    spark.sessionState.conf
      .getConfString(
        IndexConstants.INDEX_NUM_BUCKETS,
        IndexConstants.INDEX_NUM_BUCKETS_DEFAULT.toString)
      .toInt
  }

  def indexLineageEnabled(spark: SparkSession): Boolean = {
    spark.sessionState.conf
      .getConfString(
        IndexConstants.INDEX_LINEAGE_ENABLED,
        IndexConstants.INDEX_LINEAGE_ENABLED_DEFAULT)
      .toBoolean
  }

  def joinV2RuleEnabled(spark: SparkSession): Boolean = {
    spark.sessionState.conf
      .getConfString(
        IndexConstants.JOIN_V2_RULE_ENABLED,
        IndexConstants.JOIN_V2_RULE_ENABLED_DEFAULT)
      .toBoolean
  }
}
