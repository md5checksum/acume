package com.guavus.acume.core.query;

import java.io.InputStream
import scala.beans.BeanProperty

class DataExportResponse(@BeanProperty var inputStream: InputStream, @BeanProperty var fileType: String) {
  
  @BeanProperty
  var fileSize: Long = _

}
