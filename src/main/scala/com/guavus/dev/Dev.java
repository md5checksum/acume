package com.guavus.dev;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;

import com.guavus.mapred.common.collection.DimensionSet;
import com.guavus.mapred.common.collection.MeasureSet;

public class Dev {

	public static void readSeq(String str) throws Exception {
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(str);
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);

	
		DimensionSet key = new DimensionSet();
		MeasureSet value = new MeasureSet();
		BufferedWriter writer = new BufferedWriter(new FileWriter("/Users/archit.thakur/Documents/Code_Custom_SparkCache_Scala/del"));
		while(reader.next(key, value)){
			
			writer.write(key.toString() + "\t" + value.toString() + "\n" );
		}
		reader.close();
		writer.close();
	}
	
	public static void convertSequenceFileToText(String str) throws Exception {
		
		Configuration config = new Configuration();
		FileSystem fs = FileSystem.get(config);
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(str), config);
		BufferedWriter writer = new BufferedWriter(new FileWriter(str+".text"));
		DimensionSet key = new DimensionSet();
		MeasureSet value = new MeasureSet();
		while(reader.next(key, value)) {
			
			writer.write(key.toString() + "\t" + value.toString() + "\n" );
		}
		
		try{writer.close();} catch(Exception ex){}
//		try{reader.close();} catch(Exception ex){}
	}
}
