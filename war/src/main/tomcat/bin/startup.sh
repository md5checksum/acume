#!/bin/sh

#-------------------------------------
#The default values of the arguments in case args not provided
#-------------------------------------

ARG_MASTER_MODE=" --master yarn-client"
ARG_PROPERTIES_FILE=""
QUEUE_NAME=" --queue default"
ARG_POOLCONFIG=""

master_mode=-1
app_name=-1
prop_loc='$CLI_REPLACE_ACUMESPARKPROPERTYLOCATION$'
queue_name=-1

if [[ "$prop_loc" =~ ^\$CLI* ]]; then
    prop_loc=""
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
#-------------------------------------

ACUME_CACHE_DIR=$(echo "$SCRIPT_DIR" | md5sum | awk '{print $1}')
ARG_APP_NAME=" --name Acume-${ACUME_CACHE_DIR}"


#-------------------------------------
# Parse docbase from server.xml
#-------------------------------------

NAVIGATE_CMD="cd $SCRIPT_DIR/../conf/"
$NAVIGATE_CMD

DOCBASE=$(cat server.xml | grep -oPm1 '(?<=docBase=\")[^\"]+')
echo "Docbase = $DOCBASE"


#-------------------------------------
#Set the env variables from setenv.sh
#-------------------------------------

CATALINA_BASE="$SCRIPT_DIR/.."
if [ -r "$CATALINA_BASE/bin/setenv.sh" ]; then
    . "$CATALINA_BASE/bin/setenv.sh"
else
    echo "[WARNING] setenv.sh not present. "
fi


#-------------------------------------
#Set the catalina_out, create file
#-------------------------------------

CATALINA_OUT="$CATALINA_BASE"/logs/catalina.out
touch "$CATALINA_OUT"


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
    ARG_POOLCONFIG=$ARG_POOLCONFIG" spark.scheduler.allocation.file=$poolconfig_file"
fi


#-------------------------------------
# Finding crux jar
#-------------------------------------

num_crux_jars=$(ls -d /opt/tms/java/crux2.0-*-jar-with-dependencies.jar 2>>"$CATALINA_OUT" | wc -l )

if [ "$num_crux_jars" -eq 1 ]; then
    crux_jar=$(ls -d /opt/tms/java/crux2.0-*-jar-with-dependencies.jar )
    echo "INFO: Found crux jar $crux_jar" >> "$CATALINA_OUT"
elif [ "$num_crux_jars" -eq 0 ]; then
    echo "ERROR: Failed to find crux jar in /opt/tms/java/" >> "$CATALINA_OUT"
    exit 1
elif [ "$num_crux_jars" -gt 1 ]; then
    jars_list=$(ls -d /opt/tms/java/crux2.0-*-jar-with-dependencies.jar)
    echo "ERROR: Found multiple crux jars in /opt/tms/java/" >> "$CATALINA_OUT"
    echo "$jars_list" >> "$CATALINA_OUT"
    echo "Please remove all but one jar." >> "$CATALINA_OUT"
    exit 1
fi


#-------------------------------------
# Set ACUME_JAVA_OPTS
#-------------------------------------

echo "INGO: Setting ACUME_JAVA_OPTS" >> "$CATALINA_OUT"
CATALINA_BASE="$SCRIPT_DIR/.."
export ACUME_JAVA_OPTS="-Dcatalina.base=$CATALINA_BASE $ACUME_JAVA_OPTS -Djava.io.tmpdir=$CATALINA_BASE/temp -Dacume.global.cache.directory=$ACUME_CACHE_DIR"
echo "ACUME_JAVA_OPTS = $ACUME_JAVA_OPTS" >> "$CATALINA_OUT"


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


#-------------------------------------
# HBASE Jar for Spark Classpath
#-------------------------------------

SPARK_HBASE_JAR=$(ls -1 /opt/tms/java/hbase-spark/spark-hbase*-jar-with-dependencies.jar 2>/dev/null)
if [[ ! -z $SPARK_HBASE_JAR ]];then
    echo "INFO: Using HBASE Jar : $SPARK_HBASE_JAR" >> "$CATALINA_OUT"
else
    echo "WARNING: No SPARK HBASE Jar found at :/opt/tms/java/hbase-spark/" >> "$CATALINA_OUT"
fi


#-------------------------------------
# Set SPARK_JAVA_CLASSPATH
#-------------------------------------

echo "INFO: Setting SPARK_CLASSPATH" >> "$CATALINA_OUT"
export HADOOP_CONF_DIR="/opt/hadoop/conf"
spark_jars=$(ls -d -1 /opt/spark/lib/* 2>/dev/null | grep -v examples | xargs | sed 's/ /:/g')

colonSepUdfJarPath=$ACUMECOLONSEP_UDFPATHS

if [ ! -z $ACUMECOLONSEP_UDFPATHS ];then
    colonSepUdfJarPath=":"$colonSepUdfJarPath
fi

export SPARK_CLASSPATH="$DOCBASE/WEB-INF/classes/:$DOCBASE/WEB-INF/lib/*:$spark_jars:$SCRIPT_DIR/../lib/*:$crux_jar:-Djava.io.tmpdir=$CATALINA_BASE:/opt/tms/java/pcsaudf.jar$colonSepUdfJarPath:$SPARK_HBASE_JAR"
echo "SPARK_CLASSPATH = $SPARK_CLASSPATH" >> "$CATALINA_OUT"


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


#-------------------------------------
# Add log4j property file for executors
#-------------------------------------

ARG_EXECUTOR_LOGFILE="--files $DOCBASE/WEB-INF/classes/log4j-executor.properties,$DOCBASE/WEB-INF/classes/acume.ini"


#-------------------------------------
# Start the spark server
#-------------------------------------

cmd="sh -x /opt/spark/bin/spark-submit $ARG_APP_NAME $ARG_MASTER_MODE $QUEUE_NAME $ARG_POOLCONFIG $ARG_PROPERTIES_FILE $ARG_EXECUTOR_LOGFILE --class com.guavus.acume.tomcat.core.AcumeMain --jars `ls -d -1 $DOCBASE/WEB-INF/lib/* | sed ':a;N;$!ba;s/\n/,/g'`$udfJarPath  $DOCBASE/WEB-INF/lib/$core_jar --driver-java-options '$ACUME_JAVA_OPTS'"
echo "INFO: Starting Spark" >> "$CATALINA_OUT"
eval $cmd >> "$CATALINA_OUT" 2>&1 "&"
echo "INFO: Spark started successfully" >> "$CATALINA_OUT"
