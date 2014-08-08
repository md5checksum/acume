package com.guavus.dev;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
* A template for a custom Hive SerDe
*/
public class SequenceFileSerdeImplementation implements SerDe {
 
 private StructTypeInfo rowTypeInfo;
 private ObjectInspector rowOI;
 private List<String> colNames;
 private List<Object> row = new ArrayList<Object>();

 /**
  * An initialization function used to gather information about the table.
  * Typically, a SerDe implementation will be interested in the list of
  * column names and their types. That information will be used to help 
  * perform actual serialization and deserialization of data.
  */
 @Override
 public void initialize(Configuration conf, Properties tbl)
     throws SerDeException {
   // Get a list of the table's column names.
   String colNamesStr = tbl.getProperty(Constants.LIST_COLUMNS);
   colNames = Arrays.asList(colNamesStr.split(","));
  
   // Get a list of TypeInfos for the columns. This list lines up with
   // the list of column names.
   String colTypesStr = tbl.getProperty(Constants.LIST_COLUMN_TYPES);
   List<TypeInfo> colTypes =
       TypeInfoUtils.getTypeInfosFromTypeString(colTypesStr);
  
   rowTypeInfo =
       (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(colNames, colTypes);
   rowOI =
       TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(rowTypeInfo);
 }

 /**
  * This method does the work of deserializing a record into Java objects
  * that Hive can work with via the ObjectInspector interface.
  */
 @Override
 public Object deserialize(Writable blob) throws SerDeException {
   row.clear();
   // Do work to turn the fields in the blob into a set of row fields
   return row;
 }

 /**
  * Return an ObjectInspector for the row of data
  */
 @Override
 public ObjectInspector getObjectInspector() throws SerDeException {
   return rowOI;
 }

 /**
  * Unimplemented
  */
 @Override
 public SerDeStats getSerDeStats() {
   return null;
 }

 /**
  * Return the class that stores the serialized data representation.
  */
 @Override
 public Class<? extends Writable> getSerializedClass() {
   return SequenceFileTypeClass.class;
 }

 /**
  * This method takes an object representing a row of data from Hive, and
  * uses the ObjectInspector to get the data for each column and serialize
  * it.
  */
 @Override
 public Writable serialize(Object obj, ObjectInspector oi)
     throws SerDeException {
   // Take the object and transform it into a serialized representation
   return new Text();
 }
}
