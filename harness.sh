#!/bin/bash

# Java Reflection Overrides
java_args="--add-opens=jdk.management/com.sun.management.internal=ALL-UNNAMED --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.management/com.sun.jmx.mbeanserver=ALL-UNNAMED --add-opens=jdk.internal.jvmstat/sun.jvmstat.monitor=ALL-UNNAMED --add-opens=java.base/sun.reflect.generics.reflectiveObjects=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.time=ALL-UNNAMED --add-opens=java.base/java.math=ALL-UNNAMED --add-opens=java.sql/java.sql=ALL-UNNAMED -Dfile.encoding=UTF-8 -ea -Xmx4G -Xms4G"

# Ignite Specific Args
test=$1
shift

if [[ $test == "" ]]; then
        echo "$0 <test-name> <arg1> <arg2> ..."
        exit 1
fi

printf "Starting test harness with:\n"
printf "\tTest Name: $test\n"
echo -e "\tArgs: $@"
printf "\n\tJava Version: $(java --version | head -n 1)\n"

cp_file=$(mktemp)
./mvnw -pl :test-harness compile dependency:build-classpath -Dmdep.outputFile="$cp_file"

export CLASSPATH=$(find $(pwd)/test-harness -name classes -type d | tr "\n" ":")$(cat $cp_file)
rm $cp_file

start_class=tests.$test
echo $CLASSPATH
java $java_args $start_class $config $@
#./mvnw -e -pl :ignite-core,:ignite-spring,:ignite-indexing,:ignite-calcite compile exec:java -Dexec.mainClass="$start_class" -Dexec.args="$config" 2>&1 | tee logs/$(hostname).$(date +'%Y-%m-%d-%H%M%S').log
