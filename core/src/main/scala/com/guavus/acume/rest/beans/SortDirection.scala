package com.guavus.acume.rest.beans

object SortDirection extends Enumeration {

  val ASC = new SortDirection()

  val DSC = new SortDirection()

  class SortDirection extends Val {

    def reverse(): SortDirection = {
      if (this == ASC) {
        return DSC
      }
      ASC
    }
  }

  implicit def convertValue(v: Value): SortDirection = v.asInstanceOf[SortDirection]

/*
Original Java:
package com.guavus.rubix.query;

public enum SortDirection {

	ASC,DSC;
	
	public SortDirection reverse() {
		if(this == ASC) {
			return DSC;
		}
		return ASC;
	}
}

*/
}