package com.guavus.acume.core.configuration

import java.util.Collection
import java.util.HashMap
import java.util.Map
import java.util.Set
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import AbstractConfiguration._
import scala.collection.JavaConversions._
import com.guavus.acume.core.spring.Resolver

object AbstractConfiguration {

  private var logger: Logger = LoggerFactory.getLogger(classOf[AbstractConfiguration])

  private var beans: Map[Class[_], Any] = new HashMap[Class[_], Any]()
}

abstract class AbstractConfiguration protected (private var resolver: Resolver) extends Config {

  override def getBean[T](clazz: Class[T]): T = {
    var `object` = beans.get(clazz)
    if (`object` == null) {
      `object` = resolver.getBean(clazz)
      beans.put(clazz, `object`)
    }
    `object`.asInstanceOf[T]
  }

/*
Original Java:
|**
 *
 *|
package com.guavus.rubix.configuration;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.guavus.rubix.cache.CacheType;
import com.guavus.rubix.cache.Interval;
import com.guavus.rubix.cache.Intervals;
import com.guavus.rubix.cache.TimeGranularity;
import com.guavus.rubix.core.ICube;
import com.guavus.rubix.core.IDimension;
import com.guavus.rubix.core.IMeasure;
import com.guavus.rubix.core.ITransformationContext;
import com.guavus.rubix.core.Resolver;
import com.guavus.rubix.query.IGenericDimension;
import com.guavus.rubix.rules.IRule;

|**
 * @author akhil.swain
 *|
public abstract class AbstractConfiguration implements Config {

	private static Logger logger = LoggerFactory
			.getLogger(AbstractConfiguration.class);

	private Resolver resolver;

	protected AbstractConfiguration(Resolver r) {
		resolver = r;
		loadConfig();
	}

	public IDimension getDimension(String name) {
		return resolver.getDimension(name);
	}

    public IGenericDimension getGenericDimension(IDimension dim) {
        return resolver.getBean(IGenericConfig.class).getGenericDimension(dim);
    }
	public Collection<IMeasure> getMeasure(String name) {
		return resolver.getMeasure(name);
	}

	public ITransformationContext getNewTContextInstance() {
		return resolver.getNewTContextInstance();
	}

	public Intervals getModifiedRanges(Intervals ranges, Collection<Class<? extends IRule>> ruleServices) {
		return resolver.getModifiedRanges(ranges, ruleServices);
	}

	@Override
	public Collection<Map<IDimension, Integer>> getModifiedRequestFilters(
			Collection<Map<IDimension, Integer>> requestFilters,
			Integer entityId, Interval interval) {
		return resolver.getModifiedRequestFilters(requestFilters, entityId,
				interval);
	}

	public abstract void loadConfig();

	@Override
	public Collection<ICube> getAllEntities(ICube entity, CacheType type) {
		return resolver.getAllEntities(entity, type);
	}

	@Override
	public GMsFBs getGMFContaining(ICube entity,
			Collection<Map<IDimension, Integer>> requestFilters,
			String binSource, String binClass) {
		Set<IDimension> dimensions = entity.getInputDimensions();
		Set<IMeasure> measures = entity.getInputMeasures();
		TimeGranularity timeGranularity = entity.getTimeGranularity();
		
		Map<IDimension, Integer> commonRequestFilters = new HashMap<IDimension, Integer>();
		if (requestFilters != null) {
			for (Map<IDimension, Integer> requestFilter : requestFilters) {
				if (commonRequestFilters.isEmpty()) {
					commonRequestFilters.putAll(requestFilter);
				}
				for (Map.Entry<IDimension, Integer> requestFilterEntry : requestFilter
						.entrySet()) {
					Integer val = commonRequestFilters.get(requestFilterEntry
							.getKey());
					if (val == null || !(val == requestFilterEntry.getValue())) {
						commonRequestFilters
								.remove(requestFilterEntry.getKey());
					}
				}
			}
		}

		Collection<Filter> filters = Filter
				.getMatchingFilters(commonRequestFilters);
		Collection<Granularity> granularites = Granularity
				.getMatchingGranularity(dimensions);
		if (granularites == null || granularites.size() == 0) {
			throw new NullPointerException(
					"No Granularity found for dimensions " + dimensions);
		}
		GMsFBs gmf = Bin.getNearestGMF(measures, granularites, filters,
				requestFilters,timeGranularity,binSource, binClass);
		if (gmf == null) {
			throw new NullPointerException("No GMF found for measures "
					+ measures + " , granularites " + granularites
					+ " and filters " + filters+ "  and binsource  "+binSource);
		}
		return gmf;
	
	}
    
    private static Map<Class, Object> beans = new HashMap<Class, Object>();
    
    @Override
    public <T> T getBean(Class<T> clazz)
    {
        Object object = beans.get(clazz);
        if(object == null)
        {
            // Caching bean as spring get bean creates approx 150KB of garbage per call.
            object = resolver.getBean(clazz);
            beans.put(clazz, object);
        }
        return (T)object;
    }
    
    public long getBinInterval(String binClass) {
    	throw new UnsupportedOperationException("This method is not supported for " + this.getClass().getSimpleName());
    }
}

*/
}