#!/bin/bash
maven_artifactId="spark-tools"
maven_version="1.0-SNAPSHOT"

if [[ -z $MAVEN_HOME ]]; then
    echo "warnning : MAVEN_HOME not found !"
fi
base=`dirname $0`
dependencies_dir=$base/lib
if [[ -d $dependencies_dir ]] ; then
    echo "dependencies dir has been exist skip ouputing dependencies jars"
else
    mvn dependency:copy-dependencies -DoutputDirectory=$base/lib
fi
mvn clean package

##### move from target to this path
#cp $base/target/$maven_artifactId-$maven_version.jar $base

tar -zcvf $maven_artifactId-$maven_version.tgz $base/target/$maven_artifactId-$maven_version.jar   $base/lib
rm -fr $base/lib
