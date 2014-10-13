package com.guavus.acume.core.configuration

import com.guavus.rubix.configuration.FileConfiguration
import com.guavus.rubix.configuration.InstaConfiguration
import com.guavus.rubix.configuration.TMConfiguration
import com.guavus.acume.core.AcumeConf
import com.guavus.acume.core.AcumeConf
import org.hibernate.internal.util.ConfigHelper
import com.guavus.acume.core.spring.Resolver
import com.guavus.acume.core.AcumeContext

/**
 *@author pankaj.arora
 * Starting point for all configuration of Acume core 
 */
object ConfigFactory {

  private var config: Config = new Configuration(Class.forName(AcumeContext.acumeContext.get.acumeConf.getResolver()).newInstance().asInstanceOf[Resolver])

  def getInstance(): Config = {
    config
  }

}