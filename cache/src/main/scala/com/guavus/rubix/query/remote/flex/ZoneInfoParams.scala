package com.guavus.rubix.query.remote.flex

import scala.reflect.BeanProperty
import javax.xml.bind.annotation.XmlRootElement

@XmlRootElement
class ZoneInfoParams extends Serializable {

  @BeanProperty
  var startYear: String = _

  @BeanProperty
  var endYear: String = _

  def this(startYear: String, endYear: String) = {
    this()
    this.startYear = startYear
    this.endYear = endYear
  }

/*
Original Java:
package com.guavus.rubix.query.remote.flex;

public class ZoneInfoParams {
	private String startYear;
	private String endYear;
	
	public ZoneInfoParams() {
	}
	
	public ZoneInfoParams(String startYear, String endYear) {
		super();
		this.startYear = startYear;
		this.endYear = endYear;
	}
	public String getStartYear() {
		return startYear;
	}
	public void setStartYear(String startYear) {
		this.startYear = startYear;
	}
	public String getEndYear() {
		return endYear;
	}
	public void setEndYear(String endYear) {
		this.endYear = endYear;
	}

}

*/
}