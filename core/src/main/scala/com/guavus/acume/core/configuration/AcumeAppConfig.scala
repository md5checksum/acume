package com.guavus.acume.core.configuration

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import com.guavus.acume.cache.core.TimeGranularity
import com.guavus.acume.cache.core.TimeGranularity.TimeGranularity
import com.guavus.acume.core.AcumeContext
import com.guavus.acume.core.AcumeContextTrait
import com.guavus.acume.core.AcumeService
import com.guavus.acume.core.DataService
import com.guavus.acume.core.converter.AcumeDataSourceSchema
import com.guavus.acume.core.usermanagement.DefaultPermissionTemplate
import com.guavus.qb.conf.QBConf
import com.guavus.qb.services.IQueryBuilderService
import com.guavus.rubix.user.permission.IPermissionTemplate
import com.guavus.qb.services.QueryBuilderService

object AcumeAppConfig {

  private var logger: Logger = LoggerFactory.getLogger(classOf[AcumeAppConfig])

  var DEFAULT_CLASS_LOADER: ClassLoader = Thread.currentThread().getContextClassLoader

//  try {
//    val mBeanInitializer = Class.forName("com.guavus.rubix.stats.RubixStatsMBeansInitializer")
//    val method = mBeanInitializer.getMethod("initializeBeans")
//    method.invoke(null, null.asInstanceOf[Array[Any]])
//  } catch {
//    case e: Throwable => {
//      LoggerUtils.printStackTraceInError(logger, e)
//      throw new RuntimeException(e)
//    }
//  }
}

@org.springframework.context.annotation.Configuration
class AcumeAppConfig extends AcumeAppConfigTrait {

  @Bean
  @Autowired
  def acumeService(dataService: DataService): AcumeService = {
    new AcumeService(dataService)
  }

  @Bean
  @Autowired
  def dataService(queryBuilderService : Seq[IQueryBuilderService], ac : AcumeContextTrait): DataService = {
    new DataService(queryBuilderService, ac)
  }

  @Bean
  def defaultTimeGranularity(): TimeGranularity = TimeGranularity.HOUR
  
  @Bean
  @Autowired
  override def acumeContext() : AcumeContextTrait = {
    AcumeContextTrait.acumeContext.get
  }
  
  @Bean
  @Autowired
  override def queryBuilderService(acumeContext : AcumeContextTrait) : Seq[IQueryBuilderService] = {

    List(new QueryBuilderService(new AcumeDataSourceSchema(acumeContext), new QBConf()))
  }

  @Bean
  def permissionTemplate(): IPermissionTemplate = new DefaultPermissionTemplate()

/*
Original Java:
package com.guavus.rubix.configuration;

import java.lang.reflect.Method;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import util.log.LoggerUtils;

import com.guavus.rubix.RubixWebService.IRequestValidation;
import com.guavus.rubix.RubixWebService.RequestValidation;
import com.guavus.rubix.adaptive.IAdaptivePolicyService;
import com.guavus.rubix.adaptive.RubixAdaptivePolicyRegister;
import com.guavus.rubix.adaptive.impl.AdaptivePolicyServiceImpl;
import com.guavus.rubix.authorize.AuthorisationService;
import com.guavus.rubix.authorize.IAuthoriseConfig;
import com.guavus.rubix.cache.TimeGranularity;
import com.guavus.rubix.cache.flash.FlashAllocationManager;
import com.guavus.rubix.cache.flash.NativeHelper;
import com.guavus.rubix.cache.sync.SegmentSyncManager;
import com.guavus.rubix.core.Controller;
import com.guavus.rubix.core.distribution.RubixAddressGenerator;
import com.guavus.rubix.core.distribution.RubixDistribution;
import com.guavus.rubix.permission.DefaultPermissionTemplate;
import com.guavus.rubix.query.CSVDataExporter;
import com.guavus.rubix.query.CacheAllPolicyForSubQueryCache;
import com.guavus.rubix.query.DataService;
import com.guavus.rubix.query.DummyRequestValidator;
import com.guavus.rubix.query.IRequestValidator;
import com.guavus.rubix.query.ISubQueryCachePolicy;
import com.guavus.rubix.query.QueryOptimizer;
import com.guavus.rubix.query.data.ExcludedDimensionValuesLoader;
import com.guavus.rubix.query.data.IExcludedDimensionValuesLoader;
import com.guavus.rubix.query.remote.flex.IDimensionConvertorMap;
import com.guavus.rubix.query.remote.flex.RubixService;
import com.guavus.rubix.scheduler.ISchedulerPolicy;
import com.guavus.rubix.scheduler.QueryRequestPrefetchTaskManager;
import com.guavus.rubix.search.ISearchService;
import com.guavus.rubix.search.SearchConfiguration;
import com.guavus.rubix.search.SearchDataProvider;
import com.guavus.rubix.user.permission.IPermissionTemplate;
import com.guavus.rubix.util.ConverterService;
import com.guavus.rubix.workflow.CubeManager;
import com.guavus.rubix.workflow.Flow;
import com.guavus.rubix.workflow.ICubeGenerator;
import com.guavus.rubix.workflow.ICubeManager;
import com.guavus.rubix.workflow.ICubePropertyToMultiProcessorMap;
import com.guavus.rubix.workflow.IMeasureProcessorMap;
import com.guavus.rubix.workflow.SingleNodeConfigurableCubeGenerator;


|**
 * AppConfig of application should extends this class to wire rubix specific
 * classes.
 * 
 * 
 *|
@Configuration
public abstract class RubixAppConfig {
	
	private static Logger logger = LoggerFactory.getLogger(RubixAppConfig.class);
	public static ClassLoader DEFAULT_CLASS_LOADER = Thread.currentThread().getContextClassLoader();
	
	//Below code initializes the Rubix logging MBeans
	static {
		try {
			Class<?> mBeanInitializer = Class
					.forName("com.guavus.rubix.stats.RubixStatsMBeansInitializer");
			Method method = mBeanInitializer.getMethod("initializeBeans");
			method.invoke(null, (Object[]) null);
		} catch (Throwable e) {
			LoggerUtils.printStackTraceInError(logger, e);
			throw new RuntimeException(e);
		}
	}
	
	@Bean
    @Autowired
    public Controller controller(IGenericConfig genericConfig,
            ISearchService searchService) {
        return new Controller(genericConfig, searchService);



    }

	@Bean
    public SegmentSyncManager syncManager() {
        return new SegmentSyncManager();
    }

    @Bean
    public RubixDistribution rubixDistribution() {
        return new RubixDistribution(searchService(), syncManager());
    }

    @Bean
    @Autowired
    public RubixService rubixService(IGenericConfig genericConfig,
        IDimensionConvertorMap dimensionConvertorMap,
        ISearchService searchService,
        AuthorisationService authorisationService, DataService dataService) {
        return new RubixService(genericConfig, dimensionConvertorMap,
            searchService, authorisationService, dataService);
    }

    @Bean
    public IRequestValidator requestValidator() {
        return new DummyRequestValidator();
    }
    
    @Bean
    @Autowired
    public DataService dataService(Flow flow, ISearchService searchService,
        AuthorisationService authorisationService,
        IGenericConfig genericConfig, QueryOptimizer queryOptimizer, 
        IRequestValidator requestValidator, ISubQueryCachePolicy subQueryCachePolicy) {
        return new DataService(flow, searchService, authorisationService,
            genericConfig, queryOptimizer, requestValidator, subQueryCachePolicy);
    }
    
    @Bean
    @Autowired
    public ConverterService converterService(IGenericConfig genericConfig,
            IDimensionConvertorMap dimensionConvertorMap) {
        return new ConverterService(genericConfig, dimensionConvertorMap);
    }
    
    @Bean
    @Autowired
    public ISubQueryCachePolicy subQueryCachePolicy() {
    	return new CacheAllPolicyForSubQueryCache();
    }
    
    @Bean
    @Autowired
    public ISchedulerPolicy schedulerPolicy() {
    	try{
    		return (ISchedulerPolicy)Class.forName(RubixProperties.DataPrefetchSchedulerPolicy.getValue()).newInstance();
    	} catch(Throwable t){
    		throw new IllegalArgumentException(
                    "Incorrect ISchedulerPolicy " + RubixProperties.DataPrefetchSchedulerPolicy.getValue(), t);
    	}
    }
    
    @Bean
    @Autowired
    public IRequestValidation requestValidation(){
    	return new RequestValidation();
    }
    

    @Bean
    @Autowired
    public QueryOptimizer queryOptimizer(Flow flow,
        ISearchService searchService,
        AuthorisationService authorisationService, IGenericConfig genericConfig) {
        return new QueryOptimizer(flow, genericConfig, searchService,
            authorisationService);
    }

    @Bean
    @Autowired
    public AuthorisationService authorisationService(
        IAuthoriseConfig authoriseConfig, ConverterService converterService) {
        //XXX: Initiallizing with null to be used by Scheduler.
        return new AuthorisationService(authoriseConfig, converterService);
    }
    @Bean
    public Flow flow() {
        return new Flow(cubeManager(), measureProcessorMap(),
            cubePropertyProcessorMap(), getAggregationPolicyClassName());
    }

    public abstract IGenericConfig genericConfig();

    public abstract IMeasureProcessorMap measureProcessorMap();
    
    public abstract ICubePropertyToMultiProcessorMap cubePropertyProcessorMap();

    public abstract IDimensionConvertorMap dimensionConvertorMap();
    
    @SuppressWarnings("rawtypes")
	public abstract Class getAggregationPolicyClassName();

    @Bean
    public ISearchService searchService() {
        return new SearchDataProvider(searchConfiguration());
    }


    public abstract SearchConfiguration searchConfiguration();

    @Bean
    public ICubeManager cubeManager() {
        return new CubeManager(singleNodeConfigurableCubeGenerator());
    }
    
	@Bean
	public FlashAllocationManager flashAllocationManager(){
		String uniqueIdentifier = RubixProperties.NODE_UNIQUE_IDENTIFIER.getValue();
		if(uniqueIdentifier.equals(RubixAddressGenerator.DEFAULT_UNIQUE_IDENTIFIER)){
			uniqueIdentifier=RubixAddressGenerator.getHostName();
		}
		return new FlashAllocationManager(new NativeHelper(),uniqueIdentifier);
	}
    
    @Bean
    @Autowired
    public IExcludedDimensionValuesLoader excludedDimensionValuesLoader(IGenericConfig genericConfig){
    	return new ExcludedDimensionValuesLoader(genericConfig);
    }

    public ICubeGenerator singleNodeConfigurableCubeGenerator(){
    	return new SingleNodeConfigurableCubeGenerator(cubeGenerator());
    }
    
    protected ICubeGenerator cubeGenerator(){
    	try{
    		return (ICubeGenerator)Class.forName(RubixProperties.CubeGenerator.getValue()).newInstance();
    	} catch(Throwable t){
    		throw new IllegalArgumentException(
                    "Incorrect CubeGenerator " + RubixProperties.CubeGenerator.getValue(), t);
    	}
    }
    
    public abstract IAuthoriseConfig authoriseConfig(IPermissionTemplate iTemplate);
    
    @Bean
    @Autowired
    public QueryRequestPrefetchTaskManager queryRequestPrefetchTaskManager(
        IGenericConfig genericConfig, DataService dataService,
        ICubeManager cubeManager) {
        return new QueryRequestPrefetchTaskManager(genericConfig, dataService,
            cubeManager);
    }
     

    @Bean
    public TimeGranularity defaultTimeGranularity() {
        return TimeGranularity.HOUR;
    }
    
    @Bean
	public IPermissionTemplate permissionTemplate() {
    	return new DefaultPermissionTemplate() ;
    }
    
    @Bean
	public IAdaptivePolicyService adaptivePolicyService() {
    	return new AdaptivePolicyServiceImpl();
    }
    
    @Bean
	public RubixAdaptivePolicyRegister rubixAdaptivePolicyRegister() throws Exception {
    	return new RubixAdaptivePolicyRegister(adaptivePolicyService()) ;
    }
    
    @Bean
    public CSVDataExporter csvDataExporter() {
        return new CSVDataExporter();
    }
    
}

*/
}