package com.guavus.acume.core.scheduler

import scala.collection.JavaConversions._
import com.guavus.querybuilder.cube.schema.QueryBuilderSchema
import com.guavus.querybuilder.cube.schema.ICube
import com.guavus.acume.cache.utility.Utility

class PrefetchCubeLocator(schema : QueryBuilderSchema) {

  def getPrefetchCubeConfigurations(): scala.collection.mutable.HashMap[String, List[PrefetchCubeConfiguration]] = {
    val prefetchCubeConfigurationMap = new scala.collection.mutable.HashMap[String, List[PrefetchCubeConfiguration]]()
    for(cube <- schema.getCubes()) {
      val prefetchCubeConfiguration = new PrefetchCubeConfiguration();
      prefetchCubeConfiguration.setTopCube(cube)
      prefetchCubeConfigurationMap.put(cube.getBinSourceValue(), List(prefetchCubeConfiguration))
    }
    prefetchCubeConfigurationMap
  }

/*
Original Java:
|**
 * 
 *|
package com.guavus.rubix.scheduler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import com.guavus.rubix.core.ICube;
import com.guavus.rubix.core.IDimension;
import com.guavus.rubix.core.PrefetchConfiguration;
import com.guavus.rubix.util.Utility;
import com.guavus.rubix.workflow.ICubeManager;

|**
 * @author akhil.swain
 * 
 *|
public class PrefetchCubeLocator {
    private ICubeManager cubeManager;

    public PrefetchCubeLocator(ICubeManager cubeManager) {
        this.cubeManager = cubeManager;
    }

    private List<ICube> getProfileCubes(ICube topCube) {
        List<ICube> profileCubes = new ArrayList<ICube>();
        for (ICube profileCube : cubeManager.getCubes()) {
            if (!isTopCube(profileCube)) {
                if (topCube.getOutputDimensions()
                    .containsAll(Utility.getNonStaticKeys(profileCube)) && profileCube.getTimeGranularity()
                    .getGranularity() == topCube.getTimeGranularity()
                    .getGranularity() && Utility.equalString(profileCube.getBinClass(), topCube.getBinClass()) 
                    && hasSameStaticKeys(topCube, profileCube)) {
                    profileCubes.add(profileCube);
                }
            }
        }
        return profileCubes;

    }
    
    private boolean hasSameStaticKeys(ICube topCube, ICube profileCube) {
		PrefetchConfiguration topCubePrefetchConfig = topCube
				.getPrefetchConfiguration();
		PrefetchConfiguration profileCubePrefetchConfig = profileCube
				.getPrefetchConfiguration();

		|**
		 * return true if prefetch config or keys do not exist for both cubes or keys are same, false otherwise
		 *|
		List<Map<IDimension, Integer>> topCubeKeys = topCubePrefetchConfig == null? null : topCubePrefetchConfig.getKeys(); 
		List<Map<IDimension, Integer>> profileCubeKeys = profileCubePrefetchConfig == null? null : profileCubePrefetchConfig.getKeys();
		
		if(topCubeKeys == null && profileCubeKeys == null)
			return true;
		
		if ((topCubeKeys != null && profileCubeKeys == null)
				|| (topCubeKeys == null && profileCubeKeys != null))
			return false;

		return Sets.newHashSet(topCubeKeys).equals(Sets.newHashSet(profileCubeKeys));
	}

    private boolean isTopCube(ICube cube) {
        LinkedHashSet<IDimension> keys = cube.getKeys();
        if (keys == null || keys.size() == 0) {
            return true;
        }
        PrefetchConfiguration prefetchConfiguration = cube.getPrefetchConfiguration();
        if (prefetchConfiguration == null) {
            return false;
        }
        List<Map<IDimension, Integer>> prefetchKeys = prefetchConfiguration.getKeys();
        
        //if prefetch keys contain all of cube keys, then it is a top cube
        if (!Utility.isNullOrEmpty(prefetchKeys) && 
        		prefetchKeys.iterator().next().keySet().containsAll(keys)) {
            return true;
        }
        return false;
    }
    
    private boolean isSearchCube(ICube topCube) {
        return topCube.getOutputMeasures().size()==0;
    }

    public Map<String, List<PrefetchCubeConfiguration>> getPrefetchCubeConfigurations() {
    	Map<String, List<PrefetchCubeConfiguration>> prefetchCubeConfigurationMap =  new HashMap<String, List<PrefetchCubeConfiguration>>();
    	Set<ICube> consideredProfileCubes = new HashSet<ICube>();
        for (ICube topCube : cubeManager.getCubes()) {
            if (isTopCube(topCube) && !isSearchCube(topCube)) {
            	if(topCube.getPrefetchConfiguration() != null && !topCube.getPrefetchConfiguration().isShouldPreload()) {
            		continue;
            	}
                PrefetchCubeConfiguration prefetchCubeConfiguration = new PrefetchCubeConfiguration();
                prefetchCubeConfiguration.setTopCube(topCube);
                List<ICube> profileCubes = getProfileCubes(topCube);
                //Remove profile cubes which have already been considered with another top cube
                profileCubes.removeAll(consideredProfileCubes);
                consideredProfileCubes.addAll(profileCubes);
                prefetchCubeConfiguration.setProfileCubes(profileCubes);
                String binClass = Utility.validString(topCube.getBinClass())?topCube.getBinClass():prefetchCubeConfiguration.getTopCube().getTimeGranularity().getName();
                if(prefetchCubeConfigurationMap.get(binClass) == null) {
                	prefetchCubeConfigurationMap.put(binClass, new ArrayList<PrefetchCubeConfiguration>());
                }
                prefetchCubeConfigurationMap.get(binClass).add(prefetchCubeConfiguration);
            }
        }
        return prefetchCubeConfigurationMap;
        
    }
}

*/
}