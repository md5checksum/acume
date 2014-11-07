#!/bin/bash

if [ "$#" -ne 6 ] && [ "$#" -ne 4 ]; then
  echo "Usage: ./startup.sh --master run_mode --property-file config_location --name app_name"
  exit 1
fi

ARG_MASTER_MODE=" --master local"
ARG_PROPERTIES_FILE=""
ARG_APP_NAME=" --name DEFAULT_NAME"

############
# Take command line arguments 
############
while (($#)); do
  if [ "$1" = "--master" ]; then
    ARG_MASTER_MODE=" --master $2"
    echo "ARG_MASTER_MODE = $ARG_MASTER_MODE"
  elif [ "$1" = "--properties-file" ]; then
    ARG_PROPERTIES_FILE=" --properties-file $2"
    echo "ARG_PROPERTIES_FILE = $ARG_PROPERTIES_FILE"
  elif [ "$1" = "--name" ]; then
    ARG_APP_NAME=" --name $2"
    echo "ARG_APP_NAME = $ARG_APP_NAME"
  fi
  shift
done


############
# Find the current directory location
############
SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  SCRIPT_DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$SCRIPT_DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
SCRIPT_DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
echo "SCRIPT_DIR = $SCRIPT_DIR"


############
# Parse docbase from server.xml
############
NAVIGATE_CMD="cd $SCRIPT_DIR/../conf/"
$NAVIGATE_CMD

DOCBASE=$(cat server.xml | grep -oPm1 '(?<=docBase=\")[^\"]+')
echo "Docbase = $DOCBASE"

############
Finding crux jar
############
num_crux_jars=$(ls -d /opt/tms/java/crux2.0-*-jar-with-dependencies.jar)


if [ "$num_crux_jars" -eq "0" ]; then
   echo "Failed to find crux jar in /opt/tms/java/"
   exit 1
fi

if [ "$num_crux_jars" -gt "1" ]; then
   jars_list=$(ls -d /opt/tms/java/crux2.0-*-jar-with-dependencies.jar)
   echo "Found multiple crux jars in /opt/tms/java/"
   echo "$jars_list"
   echo "Please remove all but one jar."
   exit 1
fi

if [ "$num_crux_jars" -eq "1" ]; then
   crux_jar=$(ls -d /opt/tms/java/crux2.0-*-jar-with-dependencies.jar)
   echo "Found crux jar $crux_jar"
fi


############
# Set SPARK_JAVA_OPTS
############
echo "Setting SPARK_JAVA_OPTS..."
CATALINA_BASE="$SCRIPT_DIR/.."
export SPARK_JAVA_OPTS="-Dcatalina.base=$CATALINA_BASE $JAVA_OPTS"
echo "SPARK_JAVA_OPTS = $SPARK_JAVA_OPTS"


############
# Set SPARK_JAVA_CLASSPATH
############
echo "Setting SPARK_CLASSPATH..."
export SPARK_CLASSPATH="$DOCBASE/WEB-INF/classes/:$DOCBASE/WEB-INF/lib/*:/opt/spark/lib/*:$SCRIPT_DIR/../lib/*:$crux_jar"
echo "SPARK_CLASSPATH = $SPARK_CLASSPATH"


############
# Find the core jar to be used
############

assembly_folder="$DOCBASE/WEB-INF/lib/"
num_core_jars=$(ls "$assembly_folder" | grep "^core-*.*jar" | wc -l)


if [ "$num_core_jars" -eq "0" ]; then
  echo "Failed to find core jar in $assembly_folder"
  exit 1
fi

if [ "$num_core_jars" -gt "1" ]; then
  jars_list=$(ls "$assembly_folder" | grep "^core-*.*jar")
  echo "Found multiple core jars in $assembly_folder:"
  echo "$jars_list"
  echo "Please remove all but one jar."
  exit 1
fi

if [ "$num_core_jars" -eq "1" ]; then
  core_jar=$(ls "$assembly_folder" | grep "^core-*.*jar")
  echo "Found core jar $core_jar"
fi


############
# Start the spark server
############
cmd="sh -x /opt/spark/bin/spark-submit $ARG_APP_NAME $ARG_MASTER_MODE $ARG_PROPERTIES_FILE --class com.guavus.acume.tomcat.core.AcumeMain $DOCBASE/WEB-INF/lib/$core_jar"
echo "Starting Spark..."
$cmd &
echo "Spark started successfully..."

