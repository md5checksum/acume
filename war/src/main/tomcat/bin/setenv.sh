if [ $CLI_REPLACE_DEBUGPORT$ == 0 ]; then
        DEBUG_OPTS=""
else
        DEBUG_OPTS=" -Xdebug -Xrunjdwp:transport=dt_socket,address=$CLI_REPLACE_DEBUGPORT$,server=y,suspend=n"
fi

export JAVA_OPTS=$DEBUG_OPTS
