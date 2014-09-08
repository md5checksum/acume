package com.guavus.acume.flex;

import com.guavus.acume.cds.ByteBuffer;

public enum MeasureConstants {
    UNKNOWN_MEASURE(0), EMPTY_BYTEBUFFER(ByteBuffer.allocate(256 * 4)), UNKNOWN_GROWTH(-200);
    int ord_;
    ByteBuffer buff_;

    MeasureConstants(int ord) {
        ord_ = ord;
    }
    
    MeasureConstants(ByteBuffer buff) {
        buff_ = buff;
    }

    public int getValue() {
        return ord_;
    }
    
    public ByteBuffer getBitArrayValue(){
        return buff_;
    }
}
