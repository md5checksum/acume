package com.guavus.acume.core

import java.io.StringReader
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.cache.utility.SQLParserFactory
import com.guavus.acume.cache.workflow.AcumeCacheContextTraitUtil
import net.sf.jsqlparser.statement.select.PlainSelect
import net.sf.jsqlparser.statement.select.Select
import net.sf.jsqlparser.statement.select.PlainSelect

/**
 * @author kashish.jain
 */
abstract class DsInterpreterPolicy {
  
  def interpretDsName(query: String) : String
  
}

class DsInterpreterPolicyImpl extends DsInterpreterPolicy {
  
  def interpretDsName(query: String) : String = {
    val sql = SQLParserFactory.getParserManager
    val statement = sql.parse(new StringReader(query)).asInstanceOf[Select].getSelectBody.asInstanceOf[PlainSelect]
    
    val tableName = statement.getFromItem().toString
    
    val dsNames = AcumeCacheContextTraitUtil.cubeList.filter(cube => cube.cubeName.equalsIgnoreCase(tableName)).map(cube => cube.dataSourceName)
    val dsName : String = dsNames.size match {
      case 0 => AcumeContextTraitUtil.acumeConf.get(ConfConstants.defaultDatasource)
      case 1 => dsNames.get(0).get//cube.get(0).get.dataSourceName
      case _ => throw new RuntimeException("Multiple Cubes found with this tablename. Cant resolve datasourceName")
    }
    
    dsName
  }
  
}