package com.guavus.acume.core.query

import java.io.BufferedWriter
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.FileInputStream
import java.io.FileWriter
import java.io.IOException
import java.math.BigDecimal
import java.util.ArrayList
import java.util.Arrays
import java.util.List
import java.util.Map
import java.util.Set
import java.util.TreeMap

import scala.collection.JavaConversions._

import org.jgroups.util.UUID
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.guavus.acume.cache.common.AcumeConstants
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.cache.utility.Utility
import com.guavus.acume.cache.workflow.RequestType
import com.guavus.acume.core.AcumeContextTrait
import com.guavus.rubix.query.remote.flex.AggregateResponse
import com.guavus.rubix.query.remote.flex.NameValue
import com.guavus.rubix.query.remote.flex.TimeseriesResponse

import javax.xml.bind.annotation.XmlRootElement

object CSVDataExporter {
  
  var logger : Logger = LoggerFactory.getLogger(classOf[CSVDataExporter])
  final var INVALID_COLUMN_VALUE : String = "NA"
  
  private def convertInputStream(response: DataExportResponse) {
	  val bao = new ByteArrayOutputStream()
	  Utility.copyStream(response.getInputStream, bao)
	  val bai = new ByteArrayInputStream(bao.toByteArray())
	  response.setInputStream(bai)
  }

  private def getAggregateFormattedColumnNames(dimensions: Map[Integer, NameValue], measures: Map[Integer, NameValue]): String = {
    val formatted = new StringBuilder()
    formatted.append(getCSV(dimensions))
    if (dimensions.isEmpty) {
      formatted.append(AcumeConstants.COMMA)
    }
    formatted.append(getCSV(measures))
    formatted.deleteCharAt(formatted.length - 1)
    formatted.append(AcumeConstants.NEW_LINE)
    formatted.toString
  }
  
  private def getTimeseriesFormattedColumnNames(dimensions: Map[Integer, NameValue], measures: Map[Integer, NameValue]): String = {
    val formatted = new StringBuilder()
    formatted.append(getCSV(dimensions))
    formatted.append(AcumeConstants.TIMESTAMP + AcumeConstants.COMMA)
    formatted.append(getCSV(measures))
    formatted.deleteCharAt(formatted.length - 1)
    formatted.append(AcumeConstants.NEW_LINE)
    formatted.toString
  }
 
  private def getFormattedResponseTotal(response: AggregateResponse, dimensions: Map[Integer, NameValue], measures: Map[Integer, NameValue]): String = {
    if ((response == null) || response.getResults.isEmpty) {
      return ""
    }
    val formatted = new StringBuilder(AcumeConstants.TOTAL)
    val dimensionsSize = dimensions.keySet.size
    for (i <- 0 until dimensionsSize) {
      formatted.append(AcumeConstants.COMMA)
    }
    if (dimensionsSize == 0) {
      formatted.append(AcumeConstants.COMMA)
    }
    formatted.append(getCSV(measures, response.getResponseMeasures, response.getResults.get(0).getMeasures))
    formatted.deleteCharAt(formatted.length - 1)
    formatted.append(AcumeConstants.NEW_LINE)
    formatted.toString
  }
  
  private def getCSV(map: Map[Integer, NameValue]): String = {
    val string = new StringBuilder()
    for (i <- map.keySet) {
      string.append(map.get(i))
      string.append(AcumeConstants.COMMA)
    }
    string.toString
  }
  
  private def getFormattedResponseTotal(response: TimeseriesResponse, dimensions: Map[Integer, NameValue], measures: Map[Integer, NameValue]): String = {
    if ((response == null) || response.getResults.isEmpty) {
      return ""
    }
    val formatted = new StringBuilder()
    val dimensionsSize = dimensions.keySet.size
    val timeZone = AcumeContextTrait.acumeContext.get.acumeConf().get(ConfConstants.timezone)
    val calendar = Utility.getCalendar(timeZone)
    for (i <- 0 until response.getTimestamps.size) {
      for (j <- 0 until dimensionsSize) {
        formatted.append(AcumeConstants.COMMA)
      }
      val time = response.getTimestamps.get(i)
      val timestamp = Utility.getTimeInHumanReadableForm(time, timeZone, calendar)
      formatted.append(timestamp)
      formatted.append(AcumeConstants.COMMA)
      for ((key, value) <- measures) {
        val list = response.getResults.get(0).getMeasures
        if (response.getResponseMeasures.contains(value.getName)) {
          val index = response.getResponseMeasures.indexOf(value.getName)
          formatted.append(list.get(index).get(i))
        } else {
          formatted.append(INVALID_COLUMN_VALUE)
        }
        formatted.append(AcumeConstants.COMMA)
      }
      formatted.deleteCharAt(formatted.length - 1)
      formatted.append(AcumeConstants.NEW_LINE)
    }
    formatted.toString
  }
  

  private def getCSV(columnIndex: Set[Integer], columns: List[_]): String = {
    val string = new StringBuilder()
    for (i <- columnIndex) {
      if (columns(i).isInstanceOf[java.lang.Double]) {
        string.append(new BigDecimal(columns(i).asInstanceOf[java.lang.Double]))
      } else {
        string.append(columns(i).toString)
      }
      string.append(AcumeConstants.COMMA)
    }
    string.toString
  }

  private def getCSV(columnsMap: Map[Integer, NameValue], validColumnNames: List[String], validColumnValues: List[_]): String = {
    val string = new StringBuilder()
    for ((key, value) <- columnsMap) {
      if (validColumnNames.contains(value.asInstanceOf[NameValue].getName)) {
        val columnName : String = value.asInstanceOf[NameValue].getName
        val index = validColumnNames.indexOf(columnName)
        val values = validColumnValues(index)
        if (values.isInstanceOf[java.lang.Double]) {
          string.append(new BigDecimal(values.asInstanceOf[java.lang.Double]))
        } else {
          string.append(value.toString)
        }
      } else {
        string.append(INVALID_COLUMN_VALUE)
      }
      string.append(AcumeConstants.COMMA)
    }
    string.toString
  }
  
  private def getFormattedResponse(response: TimeseriesResponse, dimensions: Map[Integer, NameValue], measures: Map[Integer, NameValue]): String = {
    if (response == null) {
      return ""
    }
    val formatted = new StringBuilder()
    val timeSeriesResults = response.getResults
    val timeZone = AcumeContextTrait.acumeContext.get.acumeConf().get(ConfConstants.timezone)
    val calendar = Utility.getCalendar(timeZone)
    for (timeSeriesResult <- timeSeriesResults; i <- 0 until response.getTimestamps.size) {
      val time = response.getTimestamps.get(i)
      formatted.append(getCSV(dimensions.keySet, timeSeriesResult.getRecord))
      val timestamp = Utility.getTimeInHumanReadableForm(time, timeZone, calendar)
      formatted.append(timestamp)
      formatted.append(AcumeConstants.COMMA)
      for (j <- measures.keySet) {
        formatted.append(timeSeriesResult.getMeasures.get(j).get(i))
        formatted.append(AcumeConstants.COMMA)
      }
      formatted.deleteCharAt(formatted.length - 1)
      formatted.append(AcumeConstants.NEW_LINE)
    }
    formatted.toString
  }

  private def getFormattedResponse(response: AggregateResponse, dimensions: Map[Integer, NameValue], measures: Map[Integer, NameValue]): String = {
    if (response == null) {
      return ""
    }
    val formatted = new StringBuilder()
    val aggregateResults = response.getResults
    for (aggregateResult <- aggregateResults) {
      formatted.append(getCSV(dimensions.keySet, aggregateResult.getRecord))
      formatted.append(getCSV(measures.keySet, aggregateResult.getMeasures))
      formatted.deleteCharAt(formatted.length - 1)
      formatted.append(AcumeConstants.NEW_LINE)
    }
    formatted.toString
  }

  private def getColumnHeaders(nameValues: List[NameValue], elements: List[String], columnsToSkip: Set[String]): Map[Integer, NameValue] = {
    val columnHeaders = new TreeMap[Integer, NameValue]()
    if (Utility.isNullOrEmpty(elements)) {
      return columnHeaders
    }
    if (Utility.isNullOrEmpty(nameValues)) {
      for (i <- 0 until elements.size if columnsToSkip == null || !columnsToSkip.contains(elements.get(i))) {
        columnHeaders.put(i, new NameValue(elements.get(i), elements.get(i)))
      }
      return columnHeaders
    }
    for (i <- 0 until elements.size) {
      val element = elements.get(i)
      if (columnsToSkip == null || !columnsToSkip.contains(elements.get(i))) {
        for (nameValue <- nameValues if element == nameValue.getName) {
          columnHeaders.put(i, nameValue)
          //break
        }
      }
    }
    columnHeaders
  }

  private def createDataExporterResponse[T](queryResponse: T, 
      queryResponseTotal: T, 
      request: DataExportRequest, 
      responseDimensions: List[String], 
      responseMeasures: List[String], 
      tempDirectory: String): DataExportResponse = {
    
    val dimensionsColumnHeaders = getColumnHeaders(request.getColumnDisplayNames, responseDimensions, request.getColumnsToSkip)
    val measuresColumnHeaders = getColumnHeaders(request.getColumnDisplayNames, responseMeasures, request.getColumnsToSkip)
    var formattedColumnNames: String = null
    var formattedResponse: String = null
    var formattedResponseTotal: String = null
    if ((queryResponse.isInstanceOf[AggregateResponse]) || (queryResponseTotal.isInstanceOf[AggregateResponse])) {
      formattedColumnNames = getAggregateFormattedColumnNames(dimensionsColumnHeaders, measuresColumnHeaders)
      formattedResponse = getFormattedResponse(queryResponse.asInstanceOf[AggregateResponse], dimensionsColumnHeaders, measuresColumnHeaders)
      formattedResponseTotal = getFormattedResponseTotal(queryResponseTotal.asInstanceOf[AggregateResponse], dimensionsColumnHeaders, measuresColumnHeaders)
    } else if ((queryResponse.isInstanceOf[TimeseriesResponse]) || (queryResponseTotal.isInstanceOf[TimeseriesResponse])) {
      formattedColumnNames = getTimeseriesFormattedColumnNames(dimensionsColumnHeaders, measuresColumnHeaders)
      formattedResponse = getFormattedResponse(queryResponse.asInstanceOf[TimeseriesResponse], dimensionsColumnHeaders, measuresColumnHeaders)
      formattedResponseTotal = getFormattedResponseTotal(queryResponseTotal.asInstanceOf[TimeseriesResponse], dimensionsColumnHeaders, measuresColumnHeaders)
    } else {
      throw new RuntimeException("Invalid type of queryResponse object")
    }
    val directory = new File(tempDirectory)
    val fileName = request.getFileName
    writeToFile(formattedColumnNames + formattedResponse + formattedResponseTotal, new File(tempDirectory, getFileName(request.getFileName, FILE_TYPE.RESULTS)))
    val numOfFiles = directory.listFiles().length
    var file: File = null
    var fileType: String = null
    if (numOfFiles == 1) {
      file = directory.listFiles()(0)
      fileType = AcumeConstants.CSV
    } else if (numOfFiles == 2) {
      file = new File(directory, getFileName(fileName, FILE_TYPE.ZIP))
      val files = Arrays.asList(directory.listFiles():_*)
      ZipCreator.zip(files, file.getAbsolutePath)
      fileType = AcumeConstants.ZIP
    } else {
      throw new RuntimeException("Number of files " + numOfFiles + " not valid")
    }
    val dataExporterResponse = new DataExportResponse(new FileInputStream(file), fileType)
    dataExporterResponse
  }
  
  private def writeToFile(string: String, file: File) {
    val fw = new FileWriter(file)
    val writer = new BufferedWriter(fw)
    writer.write(string)
    writer.close()
  }

  private def generateDirectory(): String = {
    val uuid = UUID.randomUUID()
    val file = new File(AcumeConstants.TEMP_DIR, uuid.toString)
    file.mkdir()
    file.getAbsolutePath
  }

  private def deleteDirectory(tempDirectory: String) {
    val dir = new File(tempDirectory)
    val numOfFiles = dir.listFiles().length
    val files = dir.listFiles()
    for (i <- 0 until numOfFiles) {
      files(i).delete()
    }
    dir.delete()
  }
  
  private def generateFiltersFile(filters: String, tempDirectory: String, fileName: String) {
    if (filters != null) {
      val filtersFileString = getFileName(fileName, FILE_TYPE.FILTERS)
      val filtersFile = new File(tempDirectory, filtersFileString)
      writeToFile(filters, filtersFile)
    }
  }

  def getFileName(name: String, `type`: FILE_TYPE.Value): String = {
    var str = ""
    if (name == AcumeConstants.RESULTS) {
      str = AcumeConstants.UNDERSCORE + Utility.getCurrentDateInHumanReadableForm
    }
    if (`type` == FILE_TYPE.FILTERS) {
      name + "_filters" + str + AcumeConstants.DOT + AcumeConstants.CSV
    } else if (`type` == FILE_TYPE.RESULTS) {
      name + str + AcumeConstants.DOT + AcumeConstants.CSV
    } else {
      name + str + AcumeConstants.DOT + AcumeConstants.ZIP
    }
  }
  
}

class CSVDataExporter  extends IDataExporter {
  
  override def exportToFile(dataExportRequest : DataExportRequest) : DataExportResponse = {
    
    if (dataExportRequest.getRequestDataType.equals(RequestType.Aggregate)) {
      return getAggregateCSVResponse(dataExportRequest)
    } else if (dataExportRequest.getRequestDataType.equals(RequestType.Timeseries)) {
      return getTimeSeriesCSVResponse(dataExportRequest)
    }
    throw new RuntimeException("Query type not specified. Must have any one of these values " + 
        RequestType.Aggregate + "," + RequestType.Timeseries)
  }
  
  def getAggregateCSVResponse(dataExportRequest : DataExportRequest) : DataExportResponse = {
    var tempDirectory: String = null
    try {
      tempDirectory = CSVDataExporter.generateDirectory
      CSVDataExporter.generateFiltersFile(dataExportRequest.getFilterDescriptionString, tempDirectory, dataExportRequest.getFileName)
      val flexService = dataExportRequest.getRubixService
      val responses = flexService.servAggregateMultiple(new ArrayList(dataExportRequest.getRequests))
      var queryResponse: AggregateResponse = null
      var queryResponseTotal: AggregateResponse = null
      if (responses.size == 1) {
        queryResponse = responses.get(0).asInstanceOf[AggregateResponse]
        if ((queryResponse.getResponseDimensions == null) || (queryResponse.getResponseDimensions.isEmpty)) {
          queryResponseTotal = queryResponse
          queryResponse = null
        }
      } else if (responses.size == 2) {
        queryResponse = responses.get(0).asInstanceOf[AggregateResponse]
        val secondQueryResponse = responses.get(1).asInstanceOf[AggregateResponse]
        if (((queryResponse.getResponseDimensions == null) || (queryResponse.getResponseDimensions.isEmpty)) 
            && ((secondQueryResponse.getResponseDimensions != null) && (!secondQueryResponse.getResponseDimensions.isEmpty))) {
          queryResponseTotal = queryResponse
          queryResponse = secondQueryResponse
      } else if (((secondQueryResponse.getResponseDimensions == null) || (secondQueryResponse.getResponseDimensions.isEmpty)) 
          && ((queryResponse.getResponseDimensions != null) && (!queryResponse.getResponseDimensions.isEmpty))) {
        queryResponseTotal = secondQueryResponse
      } else {
        throw new IllegalArgumentException("Invalid request. Request must contain a query request object with empty dimensions")
      }
    } else {
      throw new RuntimeException("Request size is not as expected - " + responses.size)
    }
    var responseDimensions: List[String] = null
    var responseMeasures: List[String] = null
    if (queryResponse == null) {
      responseDimensions = queryResponseTotal.getResponseDimensions
      responseMeasures = queryResponseTotal.getResponseMeasures
    } else {
      responseDimensions = queryResponse.getResponseDimensions
      responseMeasures = queryResponse.getResponseMeasures
    }
    val response = CSVDataExporter.createDataExporterResponse(queryResponse, queryResponseTotal, dataExportRequest, responseDimensions, 
      responseMeasures, tempDirectory)
    CSVDataExporter.convertInputStream(response)
    response
    } catch {
      case e: IOException => throw new RuntimeException("Error creating Dataresponse object", e)
    } finally {
      CSVDataExporter.deleteDirectory(tempDirectory)
    }
  }
  
  def getTimeSeriesCSVResponse(dataExportRequest : DataExportRequest) : DataExportResponse = {
    
    var tempDirectory: String = null
    try {
      tempDirectory = CSVDataExporter.generateDirectory()
      CSVDataExporter.generateFiltersFile(dataExportRequest.getFilterDescriptionString, tempDirectory, dataExportRequest.getFileName)
      val flexService = dataExportRequest.getRubixService
      val responses = flexService.servTimeseriesMultiple(new ArrayList(dataExportRequest.getRequests))
      var queryResponse: TimeseriesResponse = null
      var queryResponseTotal: TimeseriesResponse = null
      if (responses.size == 1) {
        queryResponse = responses.get(0).asInstanceOf[TimeseriesResponse]
        if ((queryResponse.getResponseDimensions == null) || (queryResponse.getResponseDimensions.isEmpty)) {
          queryResponseTotal = queryResponse
          queryResponse = null
        }
      } else if (responses.size == 2) {
        queryResponse = responses.get(0).asInstanceOf[TimeseriesResponse]
        val secondQueryResponse = responses.get(1).asInstanceOf[TimeseriesResponse]
        if (((queryResponse.getResponseDimensions == null) || (queryResponse.getResponseDimensions.isEmpty)) 
            && ((secondQueryResponse.getResponseDimensions != null) && (!secondQueryResponse.getResponseDimensions.isEmpty))) {
          queryResponseTotal = queryResponse
          queryResponse = secondQueryResponse
        } else if (((secondQueryResponse.getResponseDimensions == null) || (secondQueryResponse.getResponseDimensions.isEmpty)) 
            && ((queryResponse.getResponseDimensions != null) && (!queryResponse.getResponseDimensions.isEmpty))) {
          queryResponseTotal = secondQueryResponse
        } else {
          throw new IllegalArgumentException("Invalid request. Request must contain a query request object with empty dimensions")
        }
      } else {
        throw new RuntimeException("Request size is not as expected")
      }
      var responseDimensions: List[String] = null
      var responseMeasures: List[String] = null
      if (queryResponse == null) {
        responseDimensions = queryResponseTotal.getResponseDimensions
        responseMeasures = queryResponseTotal.getResponseMeasures
      } else {
        responseDimensions = queryResponse.getResponseDimensions
        responseMeasures = queryResponse.getResponseMeasures
      }
      val response = CSVDataExporter.createDataExporterResponse(queryResponse, queryResponseTotal, dataExportRequest, responseDimensions, responseMeasures, tempDirectory)
      CSVDataExporter.convertInputStream(response)
      response
    } catch {
      case e: IOException => throw new RuntimeException("Error creating Dataresponse object", e)
    } finally {
      CSVDataExporter.deleteDirectory(tempDirectory)
    }
  }

}