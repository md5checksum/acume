package com.guavus.acume.core.scheduler

import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.Matchers
import org.scalatest.junit.JUnitRunner
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import com.guavus.acume.core.AcumeContext
import com.guavus.acume.core.AcumeContextTrait
import com.guavus.acume.core.DataService
import com.guavus.acume.core.AcumeConf
import com.guavus.acume.core.AcumeService
import com.guavus.acume.core.DummyContext

/**
 * @author archit.thakur
 *
 */

@RunWith(classOf[JUnitRunner])
class Scheduler extends FlatSpec with Matchers {
//  override def beforeEach {
//    deleteOutFile("src/test/resources/CloneXmlTest/test-clone/out", false)
//  }
  
  "Scheduler " should " start without Exception " in {
    val querybuilderservice = new DummyQueryBuilderService
    val acumeContext: AcumeContextTrait = new DummyContext
    val dataservice = new DataService(List(querybuilderservice), acumeContext)
    
    val querybuilder = new DummyQueryBuilderSchema
    
    val acumeconf = new AcumeConf(true, this.getClass.getResourceAsStream("/acume.conf"))
//    acumeconf.set
    
    val acumeservice = new AcumeService(dataservice);
    
    val schedulerpolicy = new VariableGranularitySchedulerPolicy(acumeconf)
    
    val x = new QueryRequestPrefetchTaskManager(dataservice, List(querybuilder), acumeconf, acumeservice, schedulerpolicy)
    x.startPrefetchScheduler
    this.synchronized {
    	this.wait()
    }
  }
  
//  override def afterEach {
//    deleteOutFile("src/test/resources/CloneXmlTest/test-clone/out",false)
//  }
}