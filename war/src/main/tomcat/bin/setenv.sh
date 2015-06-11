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
for (( i=0; i<$NUM_GC_LOG_FILES; i++ ))
  do
    if [ -s gc.log.$i ]; then
      mv gc.log.$i gc.$NOW.$i.log
    elif [ -f gc.log.$i ]; then
      rm gc.log.$i
    fi
  done

GC_OPTS=" -Xloggc:../logs/gc.log -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+UseGCLogFileRotation -XX:GCLogFileSize=500M -XX:NumberOfGCLogFiles=$NUM_GC_LOG_FILES"

ACUME_PERMGEN_CONFIG=" -XX:PermSize=$CLI_REPLACE_PERMSIZE$ -XX:MaxPermSize=$CLI_REPLACE_MAXPERMSIZE$ "
export ACUME_JAVA_OPTS=$DEBUG_OPTS$ACUME_PERMGEN_CONFIG$JMX_OPTS$GC_OPTS

