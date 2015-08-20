package com.guavus.acume.core

import java.io.StringReader
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.cache.utility.SQLParserFactory
import net.sf.jsqlparser.statement.select.PlainSelect
import net.sf.jsqlparser.statement.select.Select
import net.sf.jsqlparser.statement.select.PlainSelect
import java.util.regex.Pattern
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import com.guavus.acume.cache.workflow.AcumeCacheContextTraitUtil

/**
 * @author kashish.jain
 */
abstract class DsInterpreterPolicy {
  
  def interpretDsName(query: String) : String
  
  def updateQuery(query: String) : String
  
}

class DsInterpreterPolicyImpl extends DsInterpreterPolicy {
  
  private var logger: Logger = LoggerFactory.getLogger(classOf[DsInterpreterPolicy])
  
  def interpretDsName(query: String) : String = {
    var defaultDsName : String = AcumeContextTraitUtil.acumeConf.get(ConfConstants.defaultDatasource)
    val sql = SQLParserFactory.getParserManager
    val statement = sql.parse(new StringReader(query)).asInstanceOf[Select].getSelectBody.asInstanceOf[PlainSelect]
    val tableName = statement.getFromItem().toString

     /*
     * if tableName is global use the defaultDatasourceName
     */
    if("global".equals(tableName)){
      logger.info("Selecting defaultDatasourceName " + defaultDsName)
    	return defaultDsName
    }
      
    /*
     * Check if this is a cubeName. Use the dsName where this cube exists
     */
    val cubeNames = AcumeCacheContextTraitUtil.cubeList.filter(cube => cube.cubeName.equalsIgnoreCase(tableName))
    cubeNames.size match {
      case 1 =>
        logger.info("Selecting datasourceName " + cubeNames(0).dataSource + " interpreted from cubeNames")
        return cubeNames(0).dataSource
      case _ => logger.info("DatasourceName not found in cubes")
    }

    /*
     *  Check if this is the datasourceName. Use this dsName and update the query to make it global
     */
    val dsNames = AcumeContextTraitUtil.acumeConf.getEnabledDatasourceNames.filter(dsName => dsName.equalsIgnoreCase(tableName))
    dsNames.size match {
      case 1 =>
        logger.info("Selecting datasourceName " + dsNames(0) + " interpreted from datasourceNames")
        return dsNames(0)
      case _ => throw new RuntimeException("TableName nither a cubeName nor a datasourceName. Failing query " + query)
    }

  }
    
  def updateQuery(query: String) : String = {
    var updatedQuery : String  = query
    val dsNameRegex = "\\b" + Pattern.quote(query) + "\\b"
    AcumeContextTraitUtil.acumeConf.getEnabledDatasourceNames.map(dsName => updatedQuery = updatedQuery.replaceAll(dsNameRegex, "global"))
    updatedQuery
  }
}