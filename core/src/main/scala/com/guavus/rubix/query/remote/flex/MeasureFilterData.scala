package com.guavus.rubix.query.remote.flex

import java.io.Serializable

import java.util.ArrayList
import scala.reflect.BeanProperty

import javax.xml.bind.annotation.XmlElement
import javax.xml.bind.annotation.XmlRootElement

@SerialVersionUID(-2622636580887371088L)
@XmlRootElement
class MultiFilter extends Serializable {

  var singleFilters: ArrayList[MeasureSingleFilter] = _

  @BeanProperty
  var measure: String = _

  def this(measure: String, singleFilters: ArrayList[MeasureSingleFilter]) {
    this()
    this.measure = measure
    this.singleFilters = singleFilters
  }

  @XmlElement(`type` = classOf[ArrayList[MeasureSingleFilter]])
  def getSingleFilters(): ArrayList[MeasureSingleFilter] = singleFilters

  def setSingleFilters(singleFilters: ArrayList[MeasureSingleFilter]) {
    this.singleFilters = singleFilters
  }
}

@SerialVersionUID(-4945096459581562055L)
@XmlRootElement
class MeasureFilterData extends Serializable {
  
  @BeanProperty
  var filters: ArrayList[MultiFilter] = _
  
}