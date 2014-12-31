package com.guavus.acume.core.scheduler

import java.util.List
import scala.reflect.{BeanProperty, BooleanBeanProperty}
import com.guavus.querybuilder.cube.schema.ICube

class PrefetchCubeConfiguration {

  @BeanProperty
  var topCube: ICube = _

  @BeanProperty
  var profileCubes: List[ICube] = _

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    result = prime * result + (if ((profileCubes == null)) 0 else profileCubes.hashCode)
    result = prime * result + (if ((topCube == null)) 0 else topCube.hashCode)
    result
  }

  override def equals(obj: Any): Boolean = {
    if (this == obj) return true
    if (obj == null) return false
    if (getClass != obj.getClass) return false
    val other = obj.asInstanceOf[PrefetchCubeConfiguration]
    if (profileCubes == null) {
      if (other.profileCubes != null) return false
    } else if (profileCubes != other.profileCubes) return false
    if (topCube == null) {
      if (other.topCube != null) return false
    } else if (topCube != other.topCube) return false
    true
  }

  override def toString(): String = {
    val builder = new StringBuilder()
    builder.append("PrefetchCubeConfiguration [topCube=")
    builder.append(topCube)
    builder.append(", profileCubes=")
    builder.append(profileCubes)
    builder.append("]")
    builder.toString
  }

/*
Original Java:
|**
 * 
 *|
package com.guavus.rubix.scheduler;

import java.util.List;

import com.guavus.rubix.core.ICube;

|**
 * @author akhil.swain
 * 
 *|
public class PrefetchCubeConfiguration {
	private ICube topCube;
	private List<ICube> profileCubes;

	|**
	 * @param profileCubes
	 *            the profileCubes to set
	 *|
	public void setProfileCubes(List<ICube> profileCubes) {
		this.profileCubes = profileCubes;
	}

	|**
	 * @return the profileCubes
	 *|
	public List<ICube> getProfileCubes() {
		return profileCubes;
	}

	|**
	 * @param topCube
	 *            the topCube to set
	 *|
	public void setTopCube(ICube topCube) {
		this.topCube = topCube;
	}

	|**
	 * @return the topCube
	 *|
	public ICube getTopCube() {
		return topCube;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((profileCubes == null) ? 0 : profileCubes.hashCode());
		result = prime * result + ((topCube == null) ? 0 : topCube.hashCode());
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
		PrefetchCubeConfiguration other = (PrefetchCubeConfiguration) obj;
		if (profileCubes == null) {
			if (other.profileCubes != null)
				return false;
		} else if (!profileCubes.equals(other.profileCubes))
			return false;
		if (topCube == null) {
			if (other.topCube != null)
				return false;
		} else if (!topCube.equals(other.topCube))
			return false;
		return true;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("PrefetchCubeConfiguration [topCube=");
		builder.append(topCube);
		builder.append(", profileCubes=");
		builder.append(profileCubes);
		builder.append("]");
		return builder.toString();
	}

	

}

*/
}