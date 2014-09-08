package com.guavus.acume.cds;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.Buffer;
import java.nio.DoubleBuffer;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ByteBuffer implements Externalizable {
	public static Logger logger = LoggerFactory.getLogger(ByteBuffer.class);

    private java.nio.ByteBuffer bf;

    public ByteBuffer() {
    }
    
    public DoubleBuffer asDoubleBuffer() {
    	return bf.asDoubleBuffer();
    }
    
    public ByteBuffer asReadOnlyBuffer() {
    	return duplicate();
    }
    
    public ByteBuffer duplicate() {
    	ByteBuffer buffer = new ByteBuffer(bf.duplicate());
    	buffer.clear();
    	return buffer;
    }
    
    public ByteBuffer(java.nio.ByteBuffer byteBuffer) {
        bf = byteBuffer;
    }
    
    public ByteBuffer slice() {
    	return new ByteBuffer(bf.slice());
    }
 
    public ByteBuffer slice(boolean readOnly, int position, int limit) {
        java.nio.ByteBuffer newbf;
        if (readOnly) {
            newbf = bf.duplicate();
        } else {
            newbf = bf.duplicate();
        }
        newbf.clear();
        newbf.position(position);
        newbf.limit(limit);
        return new ByteBuffer(newbf.slice());
    }

    public ByteBuffer limit(int newLimit) {
    	bf.limit(newLimit);
    	return this;
    }
    
    public int limit() {
    	return bf.limit();
    }
    
    public ByteBuffer position(int position) {
    	bf.position(position);
    	return this;
    }
    
    public static ByteBuffer allocate(int capacity) {
        return new ByteBuffer(java.nio.ByteBuffer.allocate(capacity));
    }

    public static ByteBuffer wrap(byte[] array) {
        return new ByteBuffer(java.nio.ByteBuffer.wrap(array));
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(bf.array());
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        bf = java.nio.ByteBuffer.wrap((byte[]) in.readObject());
    }

    public java.nio.ByteBuffer getJavaByteBuffer() {
        return bf;
    }
    
    public ByteBuffer setJavaByteBuffer(java.nio.ByteBuffer bf) {
        this.bf = bf;
        return this;
    }

    public int getInt(int i) {
        return bf.getInt(i);
    }
   
    public double getDouble(int i) {
        return bf.getDouble(i);
    }

    public ByteBuffer putInt(int i, int x) {
        bf.putInt(i, x);
        return this;
    }

    public ByteBuffer put(byte[] src) {
        bf.put(src);
        return this;
    }
    
    public ByteBuffer put(ByteBuffer src) {
        bf.put(src.getJavaByteBuffer());
        return this;
    }

    public ByteBuffer rewind() {
        bf.rewind();
        return this;
    }

    public byte[] array() {
        return bf.array();
    }

    public int capacity() {
        return bf.capacity();
    }

    public Buffer clear() {
        return bf.clear();
    }

    public ByteBuffer get(byte[] dst, int offset, int length) {
        synchronized (bf) {
        	try {
            	bf.clear();
            	bf.get(dst, offset, length);
            } catch (Exception e) {
    			logger.debug("=== could not copy array ==== buffer length is: "
    					+ bf.array().length + ",dest length is:" + length);
    			throw new RuntimeException(e);
            }
            return this;
		}
    }

    public static Buffer[] toJavaBufferArray(List<ByteBuffer> byteBufferList) {
        Buffer[] buffers = new Buffer[byteBufferList.size()];
        int index = 0;
        for (ByteBuffer byteBuffer : byteBufferList) {
            buffers[index++] = byteBuffer.getJavaByteBuffer();
        }
        return buffers;
    }

    public static ByteBuffer[] toArray(List<java.nio.ByteBuffer> byteBufferList) {
        ByteBuffer[] wrapperBBArray = new ByteBuffer[byteBufferList.size()];
        int index = 0;
        for (java.nio.ByteBuffer byteBuffer : byteBufferList) {
            wrapperBBArray[index++] = new ByteBuffer(byteBuffer);
        }
        return wrapperBBArray;
    }
    
    public ByteBuffer putDouble(int index , double value) {
    	bf.putDouble(index, value);
    	return this;
    }

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((bf == null) ? 0 : bf.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ByteBuffer other = (ByteBuffer) obj;
		if (bf == null) {
			if (other.bf != null)
				return false;
		} else if (!bf.equals(other.bf))
			return false;
		return true;
	}

}
