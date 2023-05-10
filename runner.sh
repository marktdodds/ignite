#!/bin/bash

source environments/general.env

for env in "$@"; do
        source $env || exit 1
done

# Java Reflection Overrides
java_args="--add-opens=jdk.management/com.sun.management.internal=ALL-UNNAMED --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.management/com.sun.jmx.mbeanserver=ALL-UNNAMED --add-opens=jdk.internal.jvmstat/sun.jvmstat.monitor=ALL-UNNAMED --add-opens=java.base/sun.reflect.generics.reflectiveObjects=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.time=ALL-UNNAMED --add-opens=java.base/java.math=ALL-UNNAMED --add-opens=java.sql/java.sql=ALL-UNNAMED -Dfile.encoding=UTF-8 -ea -Xmx4G -Xms4G"

# Ignite Specific Args
java_args="$java_args -DIGNITE_QUIET=true -DIGNITE_UPDATE_NOTIFIER=false -DIGNITE_SYSTEM_WORKER_BLOCKED_TIMEOUT=9999999 -DIGNITE_JVM_PAUSE_DETECTOR_DISABLED=true -DIGNITE_PERFORMANCE_SUGGESTIONS_DISABLED=true"

start_class="org.apache.ignite.startup.cmdline.CommandLineStartup"
config="./config/playground-config.xml"

if [[ $IGNITE_ARGS != "" ]]; then
        java_args="$java_args $IGNITE_ARGS"
fi

if [[ $IGNITE_CONFIG != "" ]]; then
        config=$IGNITE_CONFIG
fi

if [[ ! -f $config ]]; then
        echo "Invalid config path: $config"
        exit 1
fi

printf "Starting runner with:\n"
printf "\tConfig: $config\n"
printf "\tExtra Args: $IGNITE_ARGS\n"
printf "\tJava Version: $(java --version | head -n 1)\n"

export MAVEN_OPTS="$java_args"

cp_file=$(mktemp)
logfile="logs/$(hostname).$(date +'%Y-%m-%d-%H%M%S').log"
./mvnw -pl :ignite-runner compile dependency:build-classpath -Dmdep.outputFile="$cp_file"

export CLASSPATH=$(cat $cp_file)
rm $cp_file

java $java_args $start_class $config 2>&1 | tee $logfile
#./mvnw -e -pl :ignite-core,:ignite-spring,:ignite-indexing,:ignite-calcite compile exec:java -Dexec.mainClass="$start_class" -Dexec.args="$config" 2>&1 | tee logs/$(hostname).$(date +'%Y-%m-%d-%H%M%S').log
