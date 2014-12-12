package com.guavus.rubix.query.remote.flex

import MeasureRange._

object Operator extends Enumeration {

  val GREATER_THAN = new Operator(">", "GREATER_THAN")

  val LESS_THAN = new Operator("<", "LESS_THAN")
  
  val LESSER_THAN = new Operator("<", "LESSER_THAN")

  val GREATER_THAN_EQUAL = new Operator(">=", "GREATER_THAN_EQUAL")
  
  val LESS_THAN_EQUAL = new Operator("<=", "LESS_THAN_EQUAL")
  
  val EQUAL = new Operator("=", "EQUAL")

  val INSENSITIVE_SEARCH = new Operator("", "INSENSITIVE_SEARCH")

  val CONTAINS = new Operator("like", "INSENSITIVE_SEARCH")

  val NOT_CONTAINS = new Operator("not like", "NOT_CONTAINS")

  val IN = new Operator("in", "IN")

  val STARTS_WITH = new Operator("like", "STARTS_WITH")

  val STARTS_WITH_CS = new Operator("", "STARTS_WITH_CS")

  case class Operator(sqlSymbol: String, name : String) extends Val {
    override def toString = name
  }

  implicit def convertValue(v: Value): Operator = v.asInstanceOf[Operator]

/*
Original Java:
package com.guavus.rubix.search;

import org.hibernate.criterion.Criterion;

import com.google.common.base.Function;

|**
 * @author akhil swain
 * 
 *|
public enum Operator {
    GREATER_THAN(new GreaterThanCriteraConvertor(), ">"), LESSER_THAN(
        new LesserThanCriteriaConverter(),"<"), EQUAL(new EqualCriteriaConverter(), "="), INSENSITIVE_SEARCH(
        new InsensitiveSearchCriteriaConverter(), ""), CONTAINS(
        new ContainsCriteriaConverter(false,true) ,"like"), NOT_CONTAINS(
        new NotContainsCriteriaConverter(), "not like"),IN(new InCriteriaConverter(), "in"), STARTS_WITH(
        new ContainsCriteriaConverter(true,true), "like"),
        STARTS_WITH_CS(new ContainsCriteriaConverter(true,false), "");
    
    private String sqlSymbol;
    private Function<SearchCriterion, Criterion> criteriaConverter;

    public Function<SearchCriterion, Criterion> getCriteriaConverter() {
        return criteriaConverter;
    }

    Operator(Function<SearchCriterion, Criterion> criteriaConverter, String sqlSymbol) {
        this.criteriaConverter = criteriaConverter;
        this.sqlSymbol = sqlSymbol;
    }

	public String getSqlSymbol() {
		return sqlSymbol;
	}

}

*/
}