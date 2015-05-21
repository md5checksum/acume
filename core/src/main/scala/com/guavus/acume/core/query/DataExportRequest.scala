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

object FILE_TYPE extends Enumeration {
	val FILTERS = new FILE_TYPE()
	val RESULTS = new FILE_TYPE()
	val ZIP = new FILE_TYPE()

	class FILE_TYPE extends Val

	implicit def convertValue(v: Value): FILE_TYPE = v.asInstanceOf[FILE_TYPE]
}

object EXPORT_FILE_TYPE extends Enumeration {
  
	val CSV = new EXPORT_FILE_TYPE()
	class EXPORT_FILE_TYPE extends Val
	implicit def convertValue(v: Value): EXPORT_FILE_TYPE = v.asInstanceOf[EXPORT_FILE_TYPE]
}
  
class DataExportRequest(@BeanProperty var requests: List[QueryRequest], 
    @BeanProperty var columnDisplayNames: List[NameValue], 
    @BeanProperty var filterDescriptionString: String, 
    @BeanProperty var fileType: EXPORT_FILE_TYPE.Value) {
  
  @BeanProperty
  var requestDataType: RequestType.Value = _

  @BeanProperty
  var columnsToSkip: Set[String] = _

  @BeanProperty
  var fileName: String = AcumeConstants.RESULTS

  @BeanProperty
  var outputStream: OutputStream = _

  @BeanProperty
  var rubixService: AcumeService = _

  def this(requests: List[QueryRequest], columnDisplayNames: List[NameValue], fileType: EXPORT_FILE_TYPE.Value) {
    this(requests, columnDisplayNames, null, fileType)
  }
  
  def this(requests: List[QueryRequest], fileType: EXPORT_FILE_TYPE.Value) {
    this(requests, null, fileType)
  }

}