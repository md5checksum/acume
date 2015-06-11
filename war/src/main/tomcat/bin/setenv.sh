if [ $CLI_REPLACE_DEBUGPORT$ == 0 ]; then
        DEBUG_OPTS=""
else
        DEBUG_OPTS=" -Xdebug -Xrunjdwp:transport=dt_socket,address=$CLI_REPLACE_DEBUGPORT$,server=y,suspend=n"
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

GC_OPTS="-XX:+UseG1GC -XX:MetaspaceSize=$CLI_REPLACE_MAXPERMSIZE$ -Xloggc:../logs/gc.log -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+UseGCLogFileRotation -XX:GCLogFileSize=500M -XX:NumberOfGCLogFiles=$NUM_GC_LOG_FILES"

export ACUME_JAVA_OPTS=$DEBUG_OPTS$JMX_OPTS$GC_OPTS

