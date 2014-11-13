package com.guavus.acume.core.usermanagement

import java.util.HashMap
import com.guavus.rubix.user.permission.IConverter
import java.util.LinkedHashSet
import java.util.Map
import java.util.Set
import com.guavus.rubix.user.permission.impl.AbstractPermissionTemplate
import com.guavus.rubix.user.permission.impl.DimensionPermission
import com.guavus.rubix.user.permission.impl.ServicePermission
import com.guavus.rubix.user.permission.impl.DimensionPermission
import com.guavus.rubix.user.permission.IPermission
import com.guavus.rubix.user.permission.IConverter
import com.guavus.rubix.user.permission.remote.vo.ServicePermissionConvertor
import com.guavus.rubix.user.permission.impl.DimensionPermission.DIMENSION_KEY 

class DefaultPermissionTemplate extends AbstractPermissionTemplate {

  override def getAdditionalTemplate(): Set[Set[Class[_ <: IPermission]]] = {
    val templateSet = new LinkedHashSet[Set[Class[_ <: IPermission]]]()
    val set3 = new LinkedHashSet[Class[_ <: IPermission]]()
    set3.add(classOf[ServicePermission])
    set3.add(classOf[DimensionPermission])
    templateSet.add(set3)
    templateSet
  }

  override def getPermissonConfiguration(): Map[Class[_ <: IPermission], Map[_,_]] = {
    var config = new HashMap[Class[_ <: IPermission], Map[_,_]]()
    val dimensionConfiguration = new HashMap[String, LinkedHashSet[String]]()
    val dimensions = new LinkedHashSet[String]()
    dimensionConfiguration.put(DIMENSION_KEY, dimensions)
    config = new HashMap[Class[_ <: IPermission], Map[_,_]]()
    config.put(classOf[DimensionPermission], dimensionConfiguration)
    config
  }

  override def getAdditionalConvertors(): Map[Class[_ <: IPermission], IConverter[_, _]] = {
    val map = new HashMap[Class[_ <: IPermission], IConverter[_, _]]()
    map.put(classOf[ServicePermission], new ServicePermissionConvertor())
    map
  }

  override def getAdditionalPermissions(): Map[String, Class[_ <: IPermission]] = {
    val map = new HashMap[String, Class[_ <: IPermission]]()
    map.put(ServicePermission.SERVICE_PERMISSION, classOf[ServicePermission])
    map
  }

/*
Original Java:
package com.guavus.rubix.permission;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import com.guavus.rubix.user.permission.IConverter;
import com.guavus.rubix.user.permission.IPermission;
import com.guavus.rubix.user.permission.impl.AbstractPermissionTemplate;
import com.guavus.rubix.user.permission.impl.DimensionPermission;
import com.guavus.rubix.user.permission.impl.ServicePermission;
import com.guavus.rubix.user.permission.remote.vo.ServicePermissionConvertor;
 
|**
 * This is default permission template with no special permissions. Solution might have to provide its own implementation
 * @author pankaj.arora
 *
 *|
public class DefaultPermissionTemplate extends AbstractPermissionTemplate {
 
         @Override
         public Set<Set<Class<? extends IPermission>>> getAdditionalTemplate() {
                 Set<Set<Class<? extends IPermission>>> templateSet = new LinkedHashSet<Set<Class<? extends IPermission>>>();
 
                 Set<Class<? extends IPermission>> set3 = new LinkedHashSet<Class<? extends IPermission>>();
                 set3.add(ServicePermission.class);
                 set3.add(DimensionPermission.class);
                 templateSet.add(set3);
                 return templateSet;
         }
 
         @Override
         public Map<Class<? extends IPermission>, Map> getPermissonConfiguration() {
                 Map<Class<? extends IPermission>, Map> config  = new HashMap<Class<? extends IPermission>, Map>();
                 Map dimensionConfiguration  = new HashMap();
                 Set<String> dimensions = new LinkedHashSet<String>();
                 dimensionConfiguration.put(DimensionPermission.DIMENSION_KEY, dimensions);
                 config  = new HashMap<Class<? extends IPermission>, Map>();
                 config.put(DimensionPermission.class, dimensionConfiguration);
                 return config;
                 
         }
 
         @Override
         public Map<Class<? extends IPermission>, IConverter> getAdditionalConvertors() {
        	Map<Class<? extends IPermission>, IConverter> map = new HashMap<Class<? extends IPermission>, IConverter>();
     		map.put(ServicePermission.class,
     				new ServicePermissionConvertor());
     		return map;
         }
 
         @Override
         public Map<String, Class<? extends IPermission>> getAdditionalPermissions() {
        	Map< String , Class<? extends IPermission>> map = new HashMap<String, Class<? extends IPermission>>();
     		map.put(
     				ServicePermission.SERVICE_PERMISSION, ServicePermission.class);
     		return map;
         }
         
 
 }

*/
}