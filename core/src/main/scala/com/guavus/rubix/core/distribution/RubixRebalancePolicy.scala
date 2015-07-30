package com.guavus.rubix.core.distribution

import org.infinispan.topology.DefaultRebalancePolicy

//Dummy class to avoid ERROR in logs(since RubixRebalancePolicy is hardcoded in infinispan code).
// See RC-7679
class RubixRebalancePolicy extends DefaultRebalancePolicy {
}