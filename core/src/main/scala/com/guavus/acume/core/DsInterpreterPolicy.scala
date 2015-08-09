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
    
    val tableName = statement.getInto.getName
    
    val cube = AcumeCacheContextTraitUtil.cubeList.filter(cube => cube.dataSourceName.equalsIgnoreCase(tableName))
    val dsName : String = cube.size match {
      case 0 => AcumeContextTraitUtil.acumeConf.get(ConfConstants.defaultDatasource)
      case 1 => cube.get(0).get.dataSourceName
      case _ => throw new RuntimeException("Multiple Cubes found with this tablename. Cant resolve datasourceName")
    }
    
    dsName
  }
  
}