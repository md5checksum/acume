package com.guavus.rubix.query.remote.flex

import java.io.Serializable
import javax.xml.bind.annotation.XmlRootElement
import scala.reflect.{BeanProperty, BooleanBeanProperty}

@SerialVersionUID(-9174188557495936312L)
@XmlRootElement
class MeasureSingleFilter extends Serializable {

  @BeanProperty
  var operand: Array[Double] = _

  @BeanProperty
  var operator: String = _

  def this(operator: String, operand: Array[Double]) {
    this()
    this.operator = operator
    this.operand = operand
  }

/*
Original Java:
package com.guavus.rubix.query.remote.flex;

import java.io.Serializable;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class MeasureSingleFilter implements Serializable {

	private static final long serialVersionUID = -9174188557495936312L;

	private Double[] operand;
	private String operator;
	
	public MeasureSingleFilter() {
		// TODO Auto-generated constructor stub
	}
	
	public MeasureSingleFilter(String operator, Double[] operand) {
		this.operator = operator;
		this.operand = operand;
	}
	
	public Double[] getOperand() {
		return operand;
	}
	public void setOperand(Double[] operand) {
		this.operand = operand;
	}
	public String getOperator() {
		return operator;
	}
	public void setOperator(String operator) {
		this.operator = operator;
	}
	
}

*/
}