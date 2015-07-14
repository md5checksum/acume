package com.guavus.acume.core.query;

object DataExporterUtil {
    
    def getExporterInstance(fileType : String) : IDataExporter = {
        if(fileType == "CSV") {
            return new CSVDataExporter
        }
        return null;
    }

}
