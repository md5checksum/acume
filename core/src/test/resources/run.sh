#!/bin/sh
export SPARK_JAVA_OPTS="-Dcatalina.base=/data/surbhi-acume/tomcat -Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=8087,suspend=y"
export SPARK_CLASSPATH=/data/surbhi-acume/acume-war-0.1-SNAPSHOT/WEB-INF/classes/:/data/surbhi-acume/acume-war-0.1-SNAPSHOT/WEB-INF/lib/*:/opt/spark/lib/*
sh -x ./bin/spark-submit   --name "surbhi-acume" --master local --class com.guavus.acume.tomcat.core.AcumeMain /data/surbhi-acume/acume-war-0.1-SNAPSHOT/WEB-INF/lib/core-0.1-SNAPSHOT.jar --verbose
