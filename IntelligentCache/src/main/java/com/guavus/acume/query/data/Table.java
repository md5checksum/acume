package com.guavus.acume.query.data;

import java.io.Externalizable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface Table<R, C, V> extends Externalizable {

	boolean contains(Object rowKey, Object columnKey);

    boolean containsRow(Object rowKey);

    boolean containsColumn(Object columnKey);

    V get(Object rowKey, Object columnKey);

    Integer getRow(Object rowKey);

    boolean isEmpty();

    int rowCount();

    void clear();

    V put(R rowKey, C columnKey, V value);

    Set<R> rowKeySet();

    Set<C> columnKeySet();

    void retainAll(ArrayList<R> rows);
    
    Map<C, List<V>> getColumns();
    
    Table<R, C, V> copy();
    
}