package com.guavus.acume.core.query;

trait IDataExporter {
    def exportToFile(dataExportRequest : DataExportRequest) : DataExportResponse
}
