if [ $CLI_REPLACE_DEBUGPORT$ == 0 ]; then
        DEBUG_OPTS=""
else
        DEBUG_OPTS=" -Xdebug -Xrunjdwp:transport=dt_socket,address=$CLI_REPLACE_DEBUGPORT$,server=y,suspend=n"
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

GC_OPTS=" -Xloggc:$gcRelativePath"gc.log" -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+UseGCLogFileRotation -XX:GCLogFileSize=500M -XX:NumberOfGCLogFiles=$NUM_GC_LOG_FILES "

ACUME_PERMGEN_CONFIG=" -XX:PermSize=$CLI_REPLACE_PERMSIZE$ -XX:MaxPermSize=$CLI_REPLACE_MAXPERMSIZE$ "
export ACUME_JAVA_OPTS=$DEBUG_OPTS$ACUME_PERMGEN_CONFIG$GC_OPTS

