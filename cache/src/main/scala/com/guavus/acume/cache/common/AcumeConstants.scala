package com.guavus.acume.cache.common

/**
 * @author archit.thakur
 *
 */
object AcumeConstants {

  val QUERY_RESPONSE = "queryresponse";
  val EXPORT_AGGREGATE_DATA = "exportaggregate";
  val EXPORT_TIMESERIES_DATA = "exporttimeseries";
  val EXPORT_SQL_AGGREGATE_DATA = "exportsqlaggregate";
  val FILE_NAME = "fileName";
  val TEMP_DIR = "/var/tmp";
  val TOTAL = "Total";
  val TIMESTAMP = "timestamp";
  val CSV = "csv";
  val ZIP = "zip";
  val TXT = "txt";
  val DOT = ".";
  val NEW_LINE = "\n";
  val COMMA = ",";
  val UNDERSCORE = "_";
  val RESULTS = "Results";
  val COLON = ":";
  val EQUAL_TO = "=";
  val HASH = "#";
  val NONE = "none";
  val ENCRYPTION_VALUE = "Encrypted";
  val RUN_CONFIG = "RUN_CONFIG"
  val SPARK_YARN = "SPARK_YARN"
  val TRIPLE_DOLLAR_SSC = "\\$\\$\\$" //SSC = SpecialSymbolConsidered.
  val TRIPLE_DOLLAR = "$$$"
  val Aggregate = "Aggregate"
  val Timeseries = "Timeseries"
  val DELIMITED_TAB = "\t"
  val LINE_DELIMITED = "\\|"
  val LINE = "|"
  val SPARK_SQL_SHUFFLE_PARTITIONS = "spark.sql.shuffle.partitions"
  val DISTRIBUTE_BY = " distribute by "
  val SORT_BY = " sort by "
  val PARTITIONING_ATTRIBUTES = "partitioningAttributes"
  val BUCKETING_ATTRIBUTES = "bucketingAttributes"
  val SECONDARY_INDEX = "secondaryIndex"
  val NUM_PARTITIONS = "numPartitions"
}