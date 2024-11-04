#!/bin/bash

export JAVA_HOME=/opt/java/current
export PATH=$JAVA_HOME/bin:$PATH

#SSL and Fabric example
#java -Djavax.net.ssl.keyStore=clientx-chain.jks -Djavax.net.ssl.keyStorePassword=changepass -Djavax.net.ssl.trustStore=clientx-chain.jks -Djavax.net.ssl.trustStorePassword=changepass -Dzookeeper.url=node1.redhat.com:2181 -Dzookeeper.password=admin -cp "lib/*" org.fusebyexample.mqtt.client.ThreadedProducer

java -cp "lib/*" org.fusebyexample.mqtt.client.ThreadedProducer
