package com.guavus.acume.core.query;

object DataExporterUtil {
    
    def getExporterInstance(fileType : EXPORT_FILE_TYPE.Value) : IDataExporter = {
        if(fileType.equals(EXPORT_FILE_TYPE.CSV)) {
            return new CSVDataExporter
        }
        return null;
    }

}
