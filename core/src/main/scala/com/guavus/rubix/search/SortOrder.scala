package com.guavus.rubix.search

import scala.reflect.BeanProperty
import com.guavus.rubix.query.remote.flex.SortDirection._
import com.guavus.rubix.query.remote.flex.SortDirection

class SortOrder {

  @BeanProperty
  var dimensionName: String = _

  @BeanProperty
  var sortOrder: String = _

  def this(dimensionName: String, sortOrder: String) {
    this()
    this.dimensionName = dimensionName
    this.sortOrder = sortOrder
  }

  override def toString(): String = {
    "SortOrder [dimensionName=" + dimensionName + ", sortOrder=" + sortOrder + "]"
  }
  
  def toSql() = {
    
    " order by " + dimensionName + (if(sortOrder.equalsIgnoreCase("asc")) {
      " asc "
    } else {
      " desc "
    })
  }

/*
Original Java:
|**
 * 
 *|
package com.guavus.rubix.search;

|**
 * @author Akhil Swain
 * 
 *|
public class SortOrder {

	private String dimensionName;
	private Order sortOrder;

    public SortOrder() {
    }

	public SortOrder(String dimensionName, Order sortOrder) {
		this.dimensionName = dimensionName;
		this.sortOrder = sortOrder;
	}

	public String getDimensionName() {
		return dimensionName;
	}

	public void setDimensionName(String dimensionName) {
		this.dimensionName = dimensionName;
	}

	public Order getSortOrder() {
		return sortOrder;
	}

	public void setSortOrder(Order sortOrder) {
		this.sortOrder = sortOrder;
	}
	

	@Override
	public String toString() {
		return "SortOrder [dimensionName=" + dimensionName + ", sortOrder="
				+ sortOrder + "]";
	}

}

*/
}