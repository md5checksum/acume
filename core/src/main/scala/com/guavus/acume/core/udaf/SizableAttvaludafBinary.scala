package com.guavus.acume.core.udaf

import com.guavus.attval.attvaludaf.Attvaludaf
import java.util.Comparator
import java.util.Map.Entry
import java.lang.Float

class SizableAttvaludafBinary extends Attvaludaf {
  setType("BINARY")
  
  var comparator: Comparator[Entry[Integer, Float]] = new Comparator[Entry[Integer, Float]]() {
    
    @Override
    def compare(e1: Entry[Integer, Float], e2: Entry[Integer, Float]): Int = {
      
      var v1 = e1.getValue
      var v2 = e2.getValue
      
      v2.compareTo(v1)
    }
    
  };
  
  setSize(2)
  setComparator(comparator)
}
