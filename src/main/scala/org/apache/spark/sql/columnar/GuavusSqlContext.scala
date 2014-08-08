package org.apache.spark.sql.columnar

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.columnar.InMemoryRelation
import org.apache.spark.sql.execution.ExistingRdd
import org.apache.spark.sql.execution.SparkLogicalPlan

class GuavusSqlContext(guavusSparkContext: GuavusSparkContext) extends SQLContext(guavusSparkContext) {
	
  override def cacheTable(tableName: String): Unit = {
    val currentTable = table(tableName).queryExecution.analyzed
    val asInMemoryRelation = currentTable match {
      case _: InMemoryRelation =>
        currentTable.logicalPlan

      case _ =>
        val useCompression =
          sparkContext.conf.getBoolean("spark.sql.inMemoryColumnarStorage.compressed", false)
        InMemoryRelation(useCompression, executePlan(currentTable).executedPlan)
    }

    catalog.registerTable(None, tableName, asInMemoryRelation)
  }

  /** Removes the specified table from the in-memory cache. */
  override def uncacheTable(tableName: String): Unit = {
    table(tableName).queryExecution.analyzed match {
      // This is kind of a hack to make sure that if this was just an RDD registered as a table,
      // we reregister the RDD as a table.
      case inMem @ InMemoryRelation(_, _, e: ExistingRdd) =>
        inMem.cachedColumnBuffers.unpersist()
        catalog.unregisterTable(None, tableName)
        catalog.registerTable(None, tableName, SparkLogicalPlan(e))
      case inMem: InMemoryRelation =>
        inMem.cachedColumnBuffers.unpersist()
        catalog.unregisterTable(None, tableName)
      case plan => throw new IllegalArgumentException(s"Table $tableName is not cached: $plan")
    }
  }
}