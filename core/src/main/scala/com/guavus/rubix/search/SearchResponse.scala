package com.guavus.rubix.search

import java.util._

@SerialVersionUID(-6814760797154764558L)
class SearchResponse(dimensionNames : List[String], responses : List[List[Any]]) extends Serializable {

  override def toString(): String = {
    val maxLen = 10
    val builder = new StringBuilder()
    builder.append("SearchResponse [dimensionNames=")
    builder.append(if (dimensionNames != null) dimensionNames.subList(0, Math.min(dimensionNames.size, maxLen)) else null)
    builder.append(", responses=")
    builder.append(if (responses != null) responses.subList(0, Math.min(responses.size, maxLen)) else null)
    builder.append("]")
    builder.toString
  }

/*
Original Java:
package com.guavus.rubix.search;

import java.io.Serializable;
import java.util.*;

|**
 * @author akhil swain
 * 
 *|
public class SearchResponse implements Serializable {

	private static final long serialVersionUID = -6814760797154764558L;
	private List<String> dimensionNames;
	private List<List<Object>> responses;
	public SearchResponse(){
		
	}

	public List<String> getDimensionNames() {
		return dimensionNames;
	}

	public void setDimensionNames(List<String> dimensionNames) {
		this.dimensionNames = dimensionNames;
	}

	public List<List<Object>> getResponses() {
		return responses;
	}

	public void setResponses(List<List<Object>> responses) {
		this.responses = responses;
	}

	@Override
	public String toString() {
		final int maxLen = 10;
		StringBuilder builder = new StringBuilder();
		builder.append("SearchResponse [dimensionNames=");
		builder.append(dimensionNames != null ? dimensionNames.subList(0,
				Math.min(dimensionNames.size(), maxLen)) : null);
		builder.append(", responses=");
		builder.append(responses != null ? responses.subList(0,
				Math.min(responses.size(), maxLen)) : null);
		builder.append("]");
		return builder.toString();
	}

}

*/
}