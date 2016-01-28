#!/bin/sh

#-------------------------------------
# RPM Mode Support
#-------------------------------------

. /etc/reflex/reflexpaths.sh


#-------------------------------------
#The default values of the arguments in case args not provided
#-------------------------------------

ARG_MASTER_MODE=" --master yarn-client"
ARG_PROPERTIES_FILE=""
QUEUE_NAME=" --queue default"
ARG_POOLCONFIG=""
ARG_CLASS_NAME='$CLI_REPLACE_APPCLASSNAME$'
APPPATH_WITH_COLON='$CLI_REPLACE_APPLICATIONPATH$'

master_mode=-1
app_name=-1
prop_loc='$CLI_REPLACE_ACUMESPARKPROPERTYLOCATION$'
queue_name=-1
class_name=-1
is_acume=1

if [[ "$prop_loc" =~ ^\$CLI* ]]; then
    prop_loc=""
fi

#----------------------------------
# If class not provided, its acume
#----------------------------------
if [[ "$ARG_CLASS_NAME" =~ ^\$CLI* ]]; then
    ARG_CLASS_NAME="--class com.guavus.acume.tomcat.core.AcumeMain"
else
    class_name="--class $ARG_CLASS_NAME"
    ARG_CLASS_NAME=$class_name
    is_acume=0
fi


#-------------------------------------
# Find the current directory location
#-------------------------------------

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
    SCRIPT_DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
    SOURCE="$(readlink "$SOURCE")"
    [[ $SOURCE != /* ]] && SOURCE="$SCRIPT_DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
SCRIPT_DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
echo "SCRIPT_DIR = $SCRIPT_DIR"


#-------------------------------------
# Set up unique App name and cache dir
# for Acume case
#-------------------------------------

ARG_APP_NAME=""
if [ $is_acume == 1 ]; then
    ACUME_CACHE_DIR=$(echo "$SCRIPT_DIR" | md5sum | awk '{print $1}')
    ARG_APP_NAME=" --name Acume-${ACUME_CACHE_DIR}"
fi

#-------------------------------------
# Parse docbase from server.xml
#-------------------------------------

NAVIGATE_CMD="cd $SCRIPT_DIR/../conf/"
$NAVIGATE_CMD

DOCBASE=$(cat server.xml | grep -oPm1 '(?<=docBase=\")[^\"]+')
appname_from_docbase=$(expr $DOCBASE : '\/data\/instances\/\([^\/]*\)')
echo "STARTING APP=[$appname_from_docbase] with Docbase=[$DOCBASE]"


#-------------------------------------
#Set the env variables from setenv.sh
#-------------------------------------

CATALINA_BASE="$SCRIPT_DIR/.."
if [ -r "$CATALINA_BASE/bin/setenv.sh" ]; then
    . "$CATALINA_BASE/bin/setenv.sh"
else
    echo "[WARNING] setenv.sh not present or not read. "
fi

#-------------------------------------
#Set the catalina_out, create file
#-------------------------------------

CATALINA_OUT="$CATALINA_BASE"/logs/catalina.out
touch "$CATALINA_OUT"
echo "" >> $CATALINA_OUT
echo "**********************************************************************" >> $CATALINA_OUT
echo "STARTING APP=[$appname_from_docbase] at [$(date)]" >> $CATALINA_OUT
echo "**********************************************************************" >> $CATALINA_OUT


#-------------------------------------
# Parse command line arguments
#-------------------------------------

while (($#)); do
    if [ "$1" = "--master" ]; then
        ARG_MASTER_MODE=" --master $2"
        master_mode="$2"
        echo "ARG_MASTER_MODE = $ARG_MASTER_MODE"
    elif [ "$1" = "--properties-file" ]; then
        ARG_PROPERTIES_FILE=" --properties-file $2"
        prop_loc="$2"
        echo "ARG_PROPERTIES_FILE = $ARG_PROPERTIES_FILE"
    elif [ "$1" = "--name" ]; then
        ARG_APP_NAME=" --name $2"
        app_name="$2"
        echo "ARG_APP_NAME = $ARG_APP_NAME"
    elif [ "$1" = "--queue" ]; then
        QUEUE_NAME=" --queue $2"
        queue_name="$2"
        echo "QUEUE_NAME = $QUEUE_NAME"
    fi
    shift
done

#-------------------------------------
# Set property file location
#-------------------------------------

SPARK_PROPERTYFILE=$DOCBASE/WEB-INF/classes/spark.properties

if [[ ! -z  $prop_loc ]];then
    echo "INFO: Using Configured Spark Property File : $prop_loc" >>"$CATALINA_OUT"
else
    echo "INFO: Using default Spark Property File : $SPARK_PROPERTYFILE" >>"$CATALINA_OUT"
    prop_loc="$SPARK_PROPERTYFILE"
fi

if [[ ! -f $prop_loc ]];then
    echo "ERROR: File $prop_loc does not exists" >>"$CATALINA_OUT"
    exit 1
else
    ARG_PROPERTIES_FILE=" --properties-file $prop_loc"
    echo "ARG_PROPERTIES_FILE = $ARG_PROPERTIES_FILE"
fi

#-------------------------------------
# Assigning app name
# Read the value of app name from property file
#-------------------------------------

if [[ $app_name -eq -1 ]] && [[ "$prop_loc" != "" ]]; then
    grep_cmd_output=$(cat "$prop_loc" 2>>"$CATALINA_OUT"| sed -e 's/#.*$//' | grep "spark.app.name" )
    if [[ ! -z "$grep_cmd_output" ]]; then
        echo "INFO: Picking app name from the spark conf" >> "$CATALINA_OUT"
        app_name=$( echo "$grep_cmd_output" | awk -F" " '{print $2}' )
        ARG_APP_NAME=" --name $app_name"
    fi
fi


#-------------------------------------
# Assigning master mode
# Read master mode from property location
#-------------------------------------

if [[ $master_mode -eq -1 ]] && [[ "$prop_loc" != "" ]]; then
    grep_cmd_output=$(cat "$prop_loc" 2>>"$CATALINA_OUT" | sed -e 's/#.*$//' | grep "spark.master" )
    if [[ ! -z "$grep_cmd_output" ]]; then
        echo "INFO: Picking master mode from the spark conf" >> "$CATALINA_OUT"
        master_mode=$( echo "$grep_cmd_output" | awk -F" " '{print $2}' )
        ARG_MASTER_MODE=" --master $master_mode"
    fi
fi


#-------------------------------------
# Assigning queue name
# Read queue name from property location
#-------------------------------------

if [[ ( $master_mode -eq -1 || "$master_mode" =~ ^yarn* ) && ( $queue_name -eq -1 ) && ("$prop_loc" != "") ]]; then
    grep_cmd_output=$(cat "$prop_loc" 2>>"$CATALINA_OUT" | sed -e 's/#.*$//' | grep "spark.yarn.queue" )
    if [[ ! -z "$grep_cmd_output" ]]; then
       echo "INFO: Picking queue name from the spark conf" >> "$CATALINA_OUT"
       queue_name=$( echo "$grep_cmd_output" | awk -F" " '{print $2}' )
       QUEUE_NAME=" --queue $queue_name"
    fi
fi


#-------------------------------------
# Getting the poolConfig file location and scheduler mode
#-------------------------------------

echo "prop_loc ------"$prop_loc
if [ ! -z "$prop_loc" ]; then
    ARG_POOLCONFIG=" --conf "
    poolconfig_file="$DOCBASE/WEB-INF/classes/poolConfig.xml"
    grep_poolfile=$(cat "$prop_loc" 2>>"$CATALINA_OUT" | sed -e 's/#.*$//' | grep "spark.scheduler.allocation.file" )
    if [[ ! -z "$grep_poolfile" ]]; then
        poolconfig_file=$(echo "$grep_poolfile" | awk -F" " '{print $2}' | xargs)
        if [[ "${poolconfig_file}" != "/"* ]];then
            poolconfig_file=$DOCBASE/WEB-INF/classes/$poolconfig_file
        fi
        echo "INFO: Using poolConfig file :- $poolconfig_file" >> "$CATALINA_OUT"
    else
        echo "INFO: Using default poolConfig file :- $poolconfig_file" >> "$CATALINA_OUT"
    fi
    if [ ! -f $poolconfig_file ]; then
        echo "ERROR: $poolconfig_file file does not exists" >> "$CATALINA_OUT"
        exit 1
    fi
    ARG_POOLCONFIG=$ARG_POOLCONFIG" spark.scheduler.allocation.file=$poolconfig_file "
fi

#-------------------------------------
# Finding additional user-specified jar path
#-------------------------------------
APP_JAR_PATH='$CLI_REPLACE_APP_JAR_PATH$'
if [[ "$APP_JAR_PATH" =~ ^\$CLI* ]]; then
    APP_JAR_PATH=""
else
    APP_JAR_PATH="$APP_JAR_PATH,"
fi
echo "user-specified additional jar path : $APP_JAR_PATH"

#-------------------------------------
# Finding crux jar for acume case
#-------------------------------------

if [ $is_acume == 1 ]; then
    num_crux_jars=$(ls -d ${REFLEX_ROOT_PREFIX}/opt/tms/java/crux2.0-*-jar-with-dependencies.jar 2>>"$CATALINA_OUT" | wc -l )

    if [ "$num_crux_jars" -eq 1 ]; then
            crux_jar=$(ls -d ${REFLEX_ROOT_PREFIX}/opt/tms/java/crux2.0-*-jar-with-dependencies.jar )
            echo "INFO: Found crux jar $crux_jar" >> "$CATALINA_OUT"
    elif [ "$num_crux_jars" -eq 0 ]; then
            echo "ERROR: Failed to find crux jar in ${REFLEX_ROOT_PREFIX}/opt/tms/java/" >> "$CATALINA_OUT"
            exit 1
    elif [ "$num_crux_jars" -gt 1 ]; then
            jars_list=$(ls -d ${REFLEX_ROOT_PREFIX}/opt/tms/java/crux2.0-*-jar-with-dependencies.jar)
            echo "ERROR: Found multiple crux jars in ${REFLEX_ROOT_PREFIX}/opt/tms/java/" >> "$CATALINA_OUT"
            echo "$jars_list" >> "$CATALINA_OUT"
            echo "Please remove all but one jar." >> "$CATALINA_OUT"
            exit 1
    fi
fi


#-------------------------------------
# Set JAVA_OPTS
#-------------------------------------

JAVA_OPTS='$CLI_REPLACE_JAVAOPTIONS$'
if [[ "$JAVA_OPTS" =~ ^\$CLI* ]]; then
    JAVA_OPTS=""
else
    echo "cli read JAVA_OPTS = $JAVA_OPTS"
fi

JAVA_OPTS="$JAVA_OPTS  -Dcatalina.base=$CATALINA_BASE  -Djava.io.tmpdir=$CATALINA_BASE/temp "
if [ $is_acume == 1 ]; then
    CATALINA_BASE="$SCRIPT_DIR/.."
    export ACUME_JAVA_OPTS=" -Dacume.global.cache.directory=$ACUME_CACHE_DIR $ACUME_JAVA_OPTS $JAVA_OPTS "

    #-------------------------------------
    # Check if User Provided Any
    # Driver Extra Java Options
    #-------------------------------------

    USER_DRIVER_EXTRAJAVAOPTIONS=$(cat $prop_loc 2>/dev/null |sed -e 's/#.*$//' | grep "spark.driver.extraJavaOptions" | awk -F '[ =]' '{print $2} ' | sed 's/,/:/g')

    if [[ ! -z "$USER_DRIVER_EXTRAJAVAOPTIONS" ]];then
        export ACUME_JAVA_OPTS="$USER_DRIVER_EXTRAJAVAOPTIONS $ACUME_JAVA_OPTS"
    fi

    JAVA_OPTS="$ACUME_JAVA_OPTS"
fi
echo "INFO: Setting JAVA_OPTS = $JAVA_OPTS" >> "$CATALINA_OUT"

#-------------------------------------
# Set SPARK_JAR
#-------------------------------------

echo "INFO: Setting SPARK_JAR" >> "$CATALINA_OUT"
spark_jar=$(ls "/opt/spark/lib" 2>/dev/null  | grep "^spark-assembly-*.*jar")
export SPARK_JAR="local:///opt/spark/lib/$spark_jar"
echo "SPARK_JAR = $SPARK_JAR" >> "$CATALINA_OUT"


#-------------------------------------
# Add Udf jars to classpath
#-------------------------------------

udfJarPath=""
if [ $is_acume == 1 ]; then
    dirpath="$DOCBASE/WEB-INF/classes/"
    FILE_NAME=$dirpath"acume.ini"
    prop_key="acume.global.udf.configurationxml"
    prop_value=`cat ${FILE_NAME} 2>/dev/null | grep ${prop_key} | cut -d ' ' -f2`

    if [ -z $prop_value ];then
        FILE_NAME=$dirpath"udfConfiguration.xml"
        if [ -f "$FILE_NAME" ]; then
            . $SCRIPT_DIR/getUdfJarPaths.sh --udfConfXmlPath $FILE_NAME
        else
            echo "ERROR: udfConfiguration.xml file does not exists" >>"$CATALINA_OUT"
            exit 1
        fi
    else
        . $SCRIPT_DIR/getUdfJarPaths.sh --udfConfXmlPath $prop_value
    fi

    udfJarPath=$ACUME_UDFJARPATHS
    if [ ! -z $ACUME_UDFJARPATHS ];then
        udfJarPath=,$udfJarPath
    fi
fi



#-------------------------------------
# HBASE Jar for Spark Classpath
#-------------------------------------

SPARK_HBASE_JAR=$(ls -1 ${REFLEX_ROOT_PREFIX}/opt/tms/java/hbase-spark/spark-hbase*-jar-with-dependencies.jar 2>/dev/null)
if [[ ! -z $SPARK_HBASE_JAR ]];then
    echo "INFO: Using HBASE Jar : $SPARK_HBASE_JAR" >> "$CATALINA_OUT"
else
    echo "WARNING: No SPARK HBASE Jar found at :${REFLEX_ROOT_PREFIX}/opt/tms/java/hbase-spark/" >> "$CATALINA_OUT"
fi

#------------------------------------------------
# Create apppath to be added to spark_classpath for executors
#------------------------------------------------
if [[ "$APPPATH_WITH_COLON" =~ ^\$CLI* ]]; then
    APPPATH_WITH_COLON=""
else
    APPPATH_WITH_COLON=":$APPPATH_WITH_COLON/WEB-INF/lib/*"
fi
echo "APPPATH_WITH_COLON=$APPPATH_WITH_COLON"


#-------------------------------------
# Set SPARK_JAVA_CLASSPATH
#-------------------------------------

echo "INFO: Setting SPARK_CLASSPATH" >> "$CATALINA_OUT"
export HADOOP_CONF_DIR="/opt/hadoop/conf"
spark_jars=$(ls -d -1 /opt/spark/lib/* 2>/dev/null | grep -v examples | xargs | sed 's/ /:/g')

colonSepUdfJarPath=""
if [ $is_acume == 1 ]; then
    colonSepUdfJarPath=$ACUMECOLONSEP_UDFPATHS

    if [ ! -z $ACUMECOLONSEP_UDFPATHS ];then
            colonSepUdfJarPath=":"$colonSepUdfJarPath
    fi
fi

# we will not export SPARK_CLASSPATH varibale, rather
# pass this as driver and executor options

# to be passed to driver
SPARK_CLASSPATH="$DOCBASE/WEB-INF/classes/:$DOCBASE/WEB-INF/lib/*:$spark_jars:$SCRIPT_DIR/../lib/*:$crux_jar:-Djava.io.tmpdir=$CATALINA_BASE:${REFLEX_ROOT_PREFIX}/opt/tms/java/pcsaudf.jar$colonSepUdfJarPath:$SPARK_HBASE_JAR"

if [ ! [$is_acume == 1] ]; then
        SPARK_CLASSPATH="$SPARK_CLASSPATH$APPPATH_WITH_COLON:/opt/kafka/libs/* "
fi

# to be passed to executors
ARG_POOLCONFIG="$ARG_POOLCONFIG --conf spark.executor.extraClassPath=$SPARK_CLASSPATH "

echo "INFO: SPARK_CLASSPATH = $SPARK_CLASSPATH" >> "$CATALINA_OUT"
echo "spark_classpath = $SPARK_CLASSPATH"
echo "ARG_POOLCONFIG=$ARG_POOLCONFIG"

#-------------------------------------
# Find the core jar to be used
#-------------------------------------

assembly_folder="$DOCBASE/WEB-INF/lib/"
num_core_jars=$(ls "$assembly_folder" 2>/dev/null | grep "^core-*.*jar" | wc -l)

if [[ "$num_core_jars" -eq 1 ]]; then
    core_jar=$(ls "$assembly_folder" | grep "^core-*.*jar")
    echo "INFO: Found core jar: ${assembly_folder}/${core_jar}" >> "$CATALINA_OUT"
elif [[ "$num_core_jars" -eq 0 ]]; then
    echo "ERROR: Failed to find core jar in $assembly_folder" >> "$CATALINA_OUT"
    exit 1
elif [[ "$num_core_jars" -gt 1 ]]; then
    jars_list=$(ls "$assembly_folder" | grep "^core-*.*jar")
    echo "ERROR: Found multiple core jars in $assembly_folder:" >> "$CATALINA_OUT"
    echo "$jars_list" >> "$CATALINA_OUT"
    echo "Please remove all but one jar." >> "$CATALINA_OUT"
    exit 1
fi

core_jar=$DOCBASE/WEB-INF/lib/$core_jar
echo "core_jar = $core_jar"


#---------------------------------------------------------------------------------------------
# Add log4j property file for executors and solution property file if present
#---------------------------------------------------------------------------------------------
ARG_EXECUTOR_LOGFILE="--files $DOCBASE/WEB-INF/classes/log4j-executor.properties"

if [ $is_acume == 1 ]; then
    ARG_EXECUTOR_LOGFILE="$ARG_EXECUTOR_LOGFILE,$DOCBASE/WEB-INF/classes/acume.ini"
else
    streaming_file=$DOCBASE/WEB-INF/classes/streaming.ini
    if [ -f "$streaming_file" ]; then
        ARG_EXECUTOR_LOGFILE="$ARG_EXECUTOR_LOGFILE,$streaming_file"
    fi
fi

SOLUTION_FILE=$DOCBASE/WEB-INF/classes/$CLI_REPLACE_SOLUTIONCONF$
if [ -f "$SOLUTION_FILE" ]; then
    ARG_EXECUTOR_LOGFILE="$ARG_EXECUTOR_LOGFILE,$SOLUTION_FILE"
fi

echo "--files value=$ARG_EXECUTOR_LOGFILE"

#-------------------------------------
# Run init script
# we will not return from here until
# this script returns
#-------------------------------------

INIT_SCRIPT='$CLI_REPLACE_PREINITSCRIPT$'
if [[ "$INIT_SCRIPT" =~ ^\$CLI* ]] || [[ "$INIT_SCRIPT" == "" ]]; then
    echo "INFO: No init script [$INIT_SCRIPT] found. Skipping."
else
    scrpt="sh -x '$INIT_SCRIPT'"
    echo $scrpt >> "$CATALINA_OUT"
    eval $scrpt >> "$CATALINA_OUT" 2>&1
    echo "INFO: init script $INIT_SCRIPT ran successfully" >> "$CATALINA_OUT"
    echo "INFO: init script [$INIT_SCRIPT] ran successfully"
fi

#-------------------------------------
# Start the spark server
#-------------------------------------

cmd="sh -x /opt/spark/bin/spark-submit $ARG_APP_NAME $ARG_MASTER_MODE $QUEUE_NAME $ARG_POOLCONFIG $ARG_PROPERTIES_FILE $ARG_EXECUTOR_LOGFILE $ARG_CLASS_NAME --jars $APP_JAR_PATH`ls -d -1 $DOCBASE/WEB-INF/lib/* | sed ':a;N;$!ba;s/\n/,/g'`$udfJarPath  --driver-class-path '$SPARK_CLASSPATH' --driver-java-options '$JAVA_OPTS' $core_jar"

echo "INFO: Starting Spark" >> "$CATALINA_OUT"
echo $cmd >> "$CATALINA_OUT"
eval $cmd >> "$CATALINA_OUT" 2>&1 "&"
echo "INFO: Spark started successfully" >> "$CATALINA_OUT"


