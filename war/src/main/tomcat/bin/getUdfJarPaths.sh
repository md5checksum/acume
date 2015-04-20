#!/bin/sh
udfConfXmlPath=""
while (($#)); do
  if [ "$1" = "--udfConfXmlPath" ]; then
    udfConfXmlPath="$2"
  fi
  shift
done
x="fulljarPath"
commaSeperatedJarPath=`sed -n "/$x/{s/.*<$x>\(.*\)<\/$x>.*/\1/;p}" ${udfConfXmlPath} | xargs | sed -e 's/ /,/g'`
export ACUME_UDFJARPATHS=${commaSeperatedJarPath}
colonSeperatedJarPath=`echo ${commaSeperatedJarPath} | sed 's/,/:/g'`
export ACUMECOLONSEP_UDFPATHS=${colonSeperatedJarPath}