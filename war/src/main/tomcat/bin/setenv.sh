if [ $CLI_REPLACE_DEBUGPORT$ == 0 ]; then
        DEBUG_OPTS=""
else
        DEBUG_OPTS=" -Xdebug -Xrunjdwp:transport=dt_socket,address=$CLI_REPLACE_DEBUGPORT$,server=y,suspend=n"
fi

ACUME_PERMGEN_CONFIG=" -XX:PermSize=$CLI_REPLACE_PERMSIZE$ -XX:MaxPermSize=$CLI_REPLACE_MAXPERMSIZE$ "
export ACUME_JAVA_OPTS=$DEBUG_OPTS$ACUME_PERMGEN_CONFIG

