package com.guavus.dev;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.Writable;

import com.guavus.mapred.common.collection.BufferPool;
import com.guavus.mapred.common.collection.DimensionSet;
import com.guavus.mapred.common.collection.MeasureSet;

public class SequenceFileTypeClass implements Writable {
	
	DimensionSet Ds = new DimensionSet();
	MeasureSet Ms = new MeasureSet();
	
	public SequenceFileTypeClass() {
		// TODO Auto-generated constructor stub
	}
	
	SequenceFileTypeClass(DimensionSet Ds, MeasureSet Ms){
		this.Ds = Ds;
		this.Ms = Ms;
	}
	
	public void readFrom(String str){
		
		String[] token = str.split("\t");
		Ds.readFrom(token[0]);
		Ms.readFrom(token[1]);
	}
	
	@Override
	public String toString() {
		
		return Ds.toString() + "\t" + Ms.toString();
	}

	private static BufferPool pool = new BufferPool(300);
	@Override
	public void write(DataOutput out) throws IOException {
		
//		ByteBuffer buf = pool.allocate();
//        int position = -1;
//        while (position < 0) {
//            try {
//                position = Ds.toByteBuffer(buf);
//            } catch (BufferOverflowException e) {
//                if (!pool.raiseBufferCapacity()) {
//                    throw new IOException("Cannot allocate beyond max int size");
//                }
//                buf = pool.allocate();
//            }
//        }
//        
//        int DimensionSet_position = position;
//        while (position == DimensionSet_position) {
//            try {
//                position = Ms.toByteBuffer(buf);
//            } catch (BufferOverflowException e) {
//                if (!pool.raiseBufferCapacity()) {
//                    throw new IOException("Cannot allocate beyond max int size");
//                }
//                buf = pool.allocate();
//            }
//        }
//        out.writeInt(DimensionSet_position + position + 1);
		Ds.write(out);
		out.writeChar('\t');
		Ms.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		
		
		int length = in.readInt();
        while (pool.currentPoolBufferCapacity() < length) {
            if (!pool.raiseBufferCapacity()) {
                throw new IOException("KeyCollection record exceptionally large, can't deserialize, length: " + length);
            }
        }
        ByteBuffer buf = pool.allocate();
        in.readFully(buf.array(), 0, length);
        buf.limit(length);
        Ds.readFrom(buf, length);
        in.readChar();
        length = in.readInt();
        while (pool.currentPoolBufferCapacity() < length) {
            if (!pool.raiseBufferCapacity()) {
                throw new IOException("KeyCollection record exceptionally large, can't deserialize, length: " + length);
            }
        }
        buf = pool.allocate();
        in.readFully(buf.array(), 0, length);
        buf.limit(length);
        Ms.readFrom(buf, length);
	}
}
