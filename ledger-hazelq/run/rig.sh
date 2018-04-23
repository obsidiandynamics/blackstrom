#!/bin/sh

if [ $# -lt 1 ]; then
  echo "Usage: rig.sh <Initiator|Cohort|Monitor> [JVM args]"
  exit 1
fi

role=$1
shift

cd $(dirname "$0")

jvm_args="-XX:-MaxFDLimit -XX:+TieredCompilation -XX:+UseNUMA -XX:+UseCondCardMark -XX:-UseBiasedLocking \
          -Xms2G -Xmx2G -Xss1M -Djava.net.preferIPv4Stack=true \
          -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:InitiatingHeapOccupancyPercent=0 -XX:+DisableExplicitGC \
          -Xloggc:../logs/${role}-gc-%t.log -XX:+PrintGCDetails -XX:+PrintGCTimeStamps \
          -Dlog4j.configuration=file:../../src/test/resources/log4j.properties"
cd ../..
./gradlew -x test blackstrom-hazelcast:testJar 1> /dev/null
if [ $? -ne 0 ]; then
  exit 1
fi

cd - 1> /dev/null
mkdir -p ../logs
tmp_dir="/tmp/blackstrom-$(date +%s)"
mkdir $tmp_dir
cp ../build/libs/blackstrom-hazelcast-test-*.jar $tmp_dir
java $jvm_args -cp $tmp_dir/blackstrom-hazelcast-test-*.jar $@ com.obsidiandynamics.blackstrom.rig.HazelQRig\$$role
rm -rf $tmp_dir