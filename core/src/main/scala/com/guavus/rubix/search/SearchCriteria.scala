package com.guavus.rubix.search

import java.io.Serializable
import java.util.ArrayList
import java.util.List
import org.apache.commons.collections.CollectionUtils
import scala.reflect.BeanProperty
import scala.collection.JavaConversions._
import com.google.common.collect.Lists
import java.util.Collection
import Operator._
import Join._

@SerialVersionUID(-269427656660693923L)
class SearchCriteria extends Serializable {

  @BeanProperty
  var criterionsList: List[List[SearchCriterion]] = Lists.newArrayList()

  private var criteria: List[SearchCriterion] = _

  @BeanProperty
  var joins: List[Join] = new ArrayList[Join]()

  @BeanProperty
  var sortOrder: SortOrder = _

  @Deprecated
  def addCriteria(criterion: SearchCriterion) {
    if (criterionsList.isEmpty) {
      val tempCriterions = new ArrayList[SearchCriterion]()
      criterionsList.add(tempCriterions)
    }
    criterionsList.get(0).add(criterion)
  }

  def addCriterions(criterions: List[SearchCriterion]) {
    criterionsList.add(criterions)
  }

  def addSortOrder(sortOrder: SortOrder) {
    this.sortOrder = sortOrder
  }

  def addJoin(join: Join) {
    joins.add(join)
  }

  @Deprecated
  def getLegacyCriteria(): List[SearchCriterion] = criteria

  @Deprecated
  def getCriteria(): List[SearchCriterion] = {
    if (criterionsList != null && !criterionsList.isEmpty) {
      return criterionsList.get(0)
    }
    null
  }

  @Deprecated
  def setCriteria(criteria: List[SearchCriterion]) {
    this.criterionsList = Lists.newArrayList()
    this.criterionsList.add(criteria)
  }

  override def toString(): String = {
    "SearchCriteria [criteria=" + criterionsList + ", joins=" + joins + ", sortOrder=" + sortOrder + "]"
  }

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    result = prime * result + (if ((criterionsList == null)) 0 else criterionsList.hashCode)
    result = prime * result + (if ((joins == null)) 0 else joins.hashCode)
    result = prime * result + (if ((sortOrder == null)) 0 else sortOrder.hashCode)
    result
  }

  override def equals(obj: Any): Boolean = {
    if (this == obj) return true
    if (obj == null) return false
    if (getClass != obj.getClass) return false
    val other = obj.asInstanceOf[SearchCriteria]
    if (criterionsList == null) {
      if (other.criterionsList != null) return false
    } else if (criterionsList != other.criterionsList) return false
    if (joins == null) {
      if (other.joins != null) return false
    } else if (joins != other.joins) return false
    if (sortOrder == null) {
      if (other.sortOrder != null) return false
    } else if (sortOrder != other.sortOrder) return false
    true
  }

  def toSql(): String = {
    val sql = new StringBuilder("(")
    var prefix = ""
    val valueString = new ArrayList[Any]()
    for (criterions <- criterionsList) {
      sql.append(prefix).append("(")
      var innerPrefix = ""
      for (criterion <- criterions) {
        sql.append(innerPrefix)
        if (criterion.getOperator.equalsIgnoreCase("in")) {
          for (e <- criterion.getValue.asInstanceOf[Collection[Any]]) {
            if (!(e.isInstanceOf[String])) {
              val value = e.asInstanceOf[java.lang.Double]
              valueString.add(value.intValue())
            } else {
              valueString.add("'" + e.toString + "'")
            }
          }
          sql.append(" ").append(criterion.getDimension).append(" ").append(criterion.getOperator).append(" (").append(valueString.toString.substring(1, valueString.toString.length - 1)).append(")")
        } else if (criterion.getOperator.equalsIgnoreCase(Operator.CONTAINS.name) || criterion.getOperator.equalsIgnoreCase(Operator.NOT_CONTAINS.name)) {
          sql.append(criterion.getDimension).append(" ").append(Operator.convertValue(Operator.withName(criterion.getOperator)).sqlSymbol).append(" '%").append(criterion.getValue).append("%'")
        } else if (criterion.getOperator.equalsIgnoreCase(Operator.STARTS_WITH.name)) {
          sql.append(criterion.getDimension).append(" ").append(Operator.convertValue(Operator.withName(criterion.getOperator)).sqlSymbol).append(" '").append(criterion.getValue).append("%'")
        } else {
          sql.append(criterion.getDimension).append(" ").append(Operator.convertValue(Operator.withName(criterion.getOperator)).sqlSymbol).append(" ").append((if (!(criterion.getValue.isInstanceOf[String])) criterion.getValue else "'" + criterion.getValue + "'"))
        }
        innerPrefix = " and "
      }
      sql.append(")")
      prefix = " or "
    }
    sql.append(")")
    sql.toString
  }

  def isEmpty(): Boolean = {
    if (CollectionUtils.isNotEmpty(criterionsList)) {
      for (criterions <- criterionsList if CollectionUtils.isNotEmpty(criterions)) return false
    }
    true
  }

/*
Original Java:
package com.guavus.rubix.search;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;

import com.google.inject.internal.Lists;

|**
 * @author akhil swain
 * 
 *|
public class SearchCriteria implements Serializable {
	|**
	 * 
	 *|
	
	private static final long serialVersionUID = -269427656660693923L;

	private List<List<SearchCriterion>> criterionsList;
	
	private List<SearchCriterion> criteria;

	// not using joins as of now..default is AND
	private List<Join> joins;

	private SortOrder sortOrder;

	public SearchCriteria() {
		criterionsList = Lists.newArrayList();
		joins = new ArrayList<Join>();
	}
	
	@Deprecated
	public void addCriteria(SearchCriterion criterion) {
		if (criterionsList.isEmpty()) {
			List<SearchCriterion> tempCriterions = new ArrayList<SearchCriterion>();
			criterionsList.add(tempCriterions);
		}
		
		criterionsList.get(0).add(criterion);
	}

	public void addCriterions(List<SearchCriterion> criterions) {
		criterionsList.add(criterions);
	}

	public void addSortOrder(SortOrder sortOrder) {
		this.sortOrder = sortOrder;
	}

	public void addJoin(Join join) {
		joins.add(join);
	}
	
	@Deprecated
	public List<SearchCriterion> getLegacyCriteria() {
		return criteria;
	}
	
	@Deprecated
	public List<SearchCriterion> getCriteria() {
		if (criterionsList != null && !criterionsList.isEmpty()) {
			return criterionsList.get(0);
		}
		
		return null;
	}

	public List<List<SearchCriterion>> getCriterionsList() {
		return criterionsList;
	}

	public List<Join> getJoins() {
		return joins;
	}

	public SortOrder getSortOrder() {
		return sortOrder;
	}

	public void setSortOrder(SortOrder sortOrder) {
		this.sortOrder = sortOrder;
	}
	
	@Deprecated
	public void setCriteria(List<SearchCriterion> criteria) {
		this.criterionsList = Lists.newArrayList();
		this.criterionsList.add(criteria);
	}

	public void setCriterionsList(List<List<SearchCriterion>> criterionsList) {
		this.criterionsList = criterionsList;
	}

	public void setJoins(List<Join> joins) {
		this.joins = joins;
	}

	@Override
	public String toString() {
		return "SearchCriteria [criteria=" + criterionsList + ", joins=" + joins
				+ ", sortOrder=" + sortOrder + "]";
	}

	|* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 *|
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((criterionsList == null) ? 0 : criterionsList.hashCode());
		result = prime * result + ((joins == null) ? 0 : joins.hashCode());
		result = prime * result
				+ ((sortOrder == null) ? 0 : sortOrder.hashCode());
		return result;
	}

	|* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 *|
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SearchCriteria other = (SearchCriteria) obj;
		if (criterionsList == null) {
			if (other.criterionsList != null)
				return false;
		} else if (!criterionsList.equals(other.criterionsList))
			return false;
		if (joins == null) {
			if (other.joins != null)
				return false;
		} else if (!joins.equals(other.joins))
			return false;
		if (sortOrder == null) {
			if (other.sortOrder != null)
				return false;
		} else if (!sortOrder.equals(other.sortOrder))
			return false;
		return true;
	}

	public String toSql() {
		StringBuilder sql = new StringBuilder("(");
		String prefix = "";
		ArrayList<Object> valueString = new ArrayList<Object>();
		for (List<SearchCriterion> criterions : criterionsList) {
			sql.append(prefix).append("(");
			String innerPrefix = "";
			for (SearchCriterion criterion : criterions) {
				sql.append(innerPrefix);
				if (criterion.getOperator().equalsIgnoreCase("in")) {
					for(Object e : (Collection<Object>)criterion.getValue()) 
					{
						if(!(e instanceof String)) {
							Double value = (Double)e;
							valueString.add(value.intValue());
						} else { 
							valueString.add("'" +e.toString() + "'");
						}
					}
					sql.append(" ").append(criterion.getDimension()).append(" ")
						.append(criterion.getOperator()).append(" (")
						.append(valueString.toString().substring(1, valueString.toString().length() - 1))
						.append(")");
				} else if (criterion.getOperator().equalsIgnoreCase(
						Operator.CONTAINS.name())
						|| criterion.getOperator().equalsIgnoreCase(
								Operator.NOT_CONTAINS.name())) {
					sql.append(criterion.getDimension()).append(" ")
							.append(Operator.valueOf(criterion.getOperator()).getSqlSymbol())
							.append(" '%").append(criterion.getValue()).append("%'");
				} else if (criterion.getOperator().equalsIgnoreCase(
						Operator.STARTS_WITH.name())) {
					sql.append(criterion.getDimension()).append(" ")
							.append(Operator.valueOf(criterion.getOperator()).getSqlSymbol())
							.append(" '").append(criterion.getValue()).append("%'");
				} else {
					sql.append(criterion.getDimension()).append(" ")
							.append(Operator.valueOf(criterion.getOperator()).getSqlSymbol())
							.append(" ").append((!(criterion.getValue() instanceof String)?criterion.getValue() :  "'" + criterion.getValue()+"'"));
				}
				innerPrefix = " and ";
			}
			
			sql.append(")");
			prefix = " or ";
		}
		
		sql.append(")");
		return sql.toString();
	}
	
	public boolean isEmpty() {
		if (CollectionUtils.isNotEmpty(criterionsList)) {
			for (List<SearchCriterion> criterions : criterionsList) {
				if (CollectionUtils.isNotEmpty(criterions))
					return false;
			}
		}

		return true;
	}
	

}

*/
}