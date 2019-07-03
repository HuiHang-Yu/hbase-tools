#!/bin/bash
maven_artifactId="hbase-tools"
maven_version="region-checker-2.1.1"
if [[ -z $MAVEN_HOME ]]; then
    echo "warnning : MAVEN_HOME not found !"
fi
base=`dirname $0`
package_dir=$base/"hbase-tools-region-checker-2.1.1"
if [[ -d $package_dir ]] ; then
    echo "packaging to local dir "$package_dir.tgz
    rm -fr $package_dir/*
else
    mkdir $package_dir -p
fi
if [[ -d $package_dir/lib ]] ; then
    echo "dependencies dir has been exist skip ouputing dependencies jars"
else
    mvn dependency:copy-dependencies -DoutputDirectory=$package_dir/lib
fi
mvn clean package

##### move from target to this path
cp $base/target/$maven_artifactId-$maven_version.jar $package_dir
cp $base/README.md $package_dir
rm -fr $base/$maven_artifactId-$maven_version.tgz
tar -zcvf $base/$maven_artifactId-$maven_version.tgz $base/$package_dir
