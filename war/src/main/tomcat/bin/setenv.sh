if [ $CLI_REPLACE_DEBUGPORT$ == 0 ]; then
        DEBUG_OPTS=""
else
        DEBUG_OPTS=" -Xdebug -Xrunjdwp:transport=dt_socket,address=$CLI_REPLACE_DEBUGPORT$,server=y,suspend=n"
fi

if [ $CLI_REPLACE_JMXPORT$ == 0 ]; then
   		JMX_OPTS=""
   else
        JMX_OPTS=" -Djava.rmi.server.hostname=$CLI_REPLACE_IPADDRESS$ -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=$CLI_REPLACE_JMXPORT$ -Dcom.sun.management.jmxremote.authenticate=$CLI_REPLACE_JMXAUTHENTICATE$ -Dcom.sun.management.jmxremote.ssl=$CLI_REPLACE_JMXSSL$"
fi

NUM_GC_LOG_FILES=5
NOW=`date +"%Y-%m-%d.%H-%M-%S"`
gcRelativePath="../logs/"
for (( i=0; i<$NUM_GC_LOG_FILES; i++ ))
  do
    if [ -s $gcRelativePath"gc.log.$i" ]; then
      mv $gcRelativePath"gc.log.$i" $gcRelativePath"gc.$NOW.$i.log"
    elif [ -s $gcRelativePath"gc.log.$i.current" ]; then
      mv $gcRelativePath"gc.log.$i.current" $gcRelativePath"gc.$NOW.$i.log"
    elif [ -f $gcRelativePath"gc.log.$i" ]; then
      rm $gcRelativePath"gc.log.$i"
    elif [ -f $gcRelativePath"gc.log.$i.current" ]; then
      rm $gcRelativePath"gc.log.$i.current"
    fi
  done

GC_OPTS=" -XX:+UseG1GC -XX:G1HeapRegionSize=16M -XX:MetaspaceSize=$CLI_REPLACE_MAXPERMSIZE$ -Xloggc:$gcRelativePath"gc.log" -XX:+PrintFlagsFinal -XX:+PrintReferenceGC -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -verbose:gc -XX:+PrintGCDetails -XX:+PrintAdaptiveSizePolicy -XX:AdaptiveSizePolicyOutputInterval=1 -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:+UseGCLogFileRotation -XX:GCLogFileSize=500M -XX:NumberOfGCLogFiles=$NUM_GC_LOG_FILES -XX:MaxDirectMemorySize=4096M -XX:-ResizePLAB -XX:+UseCompressedOops -XX:InitiatingHeapOccupancyPercent=45 -XX:+UnlockExperimentalVMOptions -XX:G1MixedGCLiveThresholdPercent=85 "

ACUME_PERMGEN_CONFIG=" -XX:MetaspaceSize=$CLI_REPLACE_PERMSIZE$ "

export ACUME_JAVA_OPTS=$DEBUG_OPTS$ACUME_PERMGEN_CONFIG$JMX_OPTS$GC_OPTS

