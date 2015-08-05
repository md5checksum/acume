package com.guavus.acume.core.query;

import java.io.OutputStream
import java.util.List
import scala.beans.BeanProperty
import com.guavus.acume.cache.common.AcumeConstants
import com.guavus.acume.cache.workflow.RequestType
import com.guavus.acume.core.AcumeService
import com.guavus.rubix.query.remote.flex.NameValue
import com.guavus.rubix.query.remote.flex.QueryRequest
import javax.xml.bind.annotation.XmlRootElement
import com.guavus.rubix.query.remote.flex.QueryJsonUtil
import com.guavus.acume.workflow.RequestDataType
import com.guavus.acume.workflow.RequestDataType._
/*
* @author kashish.jain
*/

@XmlRootElement
class DataExportRequest {
  
  @BeanProperty 
  var requests: List[QueryRequest] = _
  
  @BeanProperty
  var columnDisplayNames: List[NameValue] = _
  
  @BeanProperty 
  var filterDescriptionString: String = _
  
  @BeanProperty 
  var fileType: String = _
  
  @BeanProperty
  var requestDataType: RequestDataType = _

  @BeanProperty
  var columnsToSkip: java.util.Set[String] = _

  @BeanProperty
  var fileName: String = AcumeConstants.RESULTS

  @BeanProperty
  var outputStream: OutputStream = _

  @BeanProperty
  var rubixService: AcumeService = _

  def this(requests: List[QueryRequest], columnDisplayNames: List[NameValue], filterDescriptionString: String, fileType: String) {
    this()
    this.requests = requests
    this.columnDisplayNames = columnDisplayNames
    this.filterDescriptionString = filterDescriptionString
    this.fileType = fileType
  }
  
  def this(requests: List[QueryRequest], columnDisplayNames: List[NameValue], fileType: String) {
    this(requests, columnDisplayNames, null, fileType)
  }
  
  def this(requests: List[QueryRequest], fileType: String) {
    this(requests, null, fileType)
  }
  
  override def toString(): String = QueryJsonUtil.dataExportRequestToJson(this)

}