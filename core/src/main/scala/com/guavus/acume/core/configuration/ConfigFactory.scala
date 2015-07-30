package com.guavus.acume.core.configuration

import com.guavus.acume.core.AcumeContextTraitUtil
import com.guavus.acume.core.spring.Resolver

/**
 *@author pankaj.arora
 * Starting point for all configuration of Acume core 
 */
object ConfigFactory {

  private var config: Config = new Configuration(Class.forName(AcumeContextTraitUtil.acumeConf.getResolver()).newInstance().asInstanceOf[Resolver])
  
  def getInstance(): Config = {
    config
  }

}