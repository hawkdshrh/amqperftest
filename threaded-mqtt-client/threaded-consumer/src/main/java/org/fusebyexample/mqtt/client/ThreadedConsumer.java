/*
 * Copyright (C) Red Hat, Inc.
 * http://www.redhat.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.fusebyexample.mqtt.client;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.fusesource.mqtt.client.QoS;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThreadedConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(ThreadedConsumer.class);

    private static URI REMOTE_ADDRESS;
    private static int MAX_READ_RATE;
    private static int RECEIVE_BUFFER_SIZE = 1024 * 64;
    private static int NUM_THREADS_PER_DESTINATION = 1;
    private static long RECONNECT_DELAY = 10;
    private static long THREAD_DELAY = 10;
    private static long MAX_RECONNECT_DELAY = 30 * 1000;
    private static long MAX_RECONNECT_ATTEMPTS = -1;
    private static long MAX_CONNECT_ATTEMPTS = -1;
    private static short KEEPALIVE = 60;
    private static String CLIENT_ID;
    private static String DESTINATIONS;
    private static String MQTT_VERSION;
    private static String USER;
    private static String PASSWORD;
    private static double RECONNECT_BACKOFF_MULTIPLIER = 2.0f;
    private static boolean CLEAN_SESSION = false;
    private static QoS MQTT_QOS;

    public static void main(String args[]) throws InterruptedException, IOException {

        try {

            Properties properties = new Properties();

            try {
                properties.load(new FileInputStream("jndi.properties"));
            } catch (IOException ex) {
                LOG.warn("jndi.properties not found, defaulting to packaged version.");
                properties.load(ThreadedConsumer.class.getResourceAsStream("/jndi.properties"));
            }

            REMOTE_ADDRESS = URI.create(properties.getProperty("remote.address"));
            DESTINATIONS = properties.getProperty("destinations");
            MAX_READ_RATE = Integer.parseInt(properties.getProperty("max.read.rate"));
            NUM_THREADS_PER_DESTINATION = Integer.parseInt(properties.getProperty("threads.per.destination"));
            RECEIVE_BUFFER_SIZE = Integer.parseInt(properties.getProperty("receive.buffer.size"));
            RECONNECT_DELAY = Long.parseLong(properties.getProperty("reconnect.delay"));
            THREAD_DELAY = Long.parseLong(properties.getProperty("thread.delay"));
            MAX_RECONNECT_DELAY = Long.parseLong(properties.getProperty("max.reconnect.delay"));
            MAX_RECONNECT_ATTEMPTS = Long.parseLong(properties.getProperty("max.reconnect.attempts"));
            MAX_CONNECT_ATTEMPTS = Long.parseLong(properties.getProperty("max.connect.attempts"));
            KEEPALIVE = Short.parseShort(properties.getProperty("mqtt.keepalive"));
            RECONNECT_BACKOFF_MULTIPLIER = Double.parseDouble(properties.getProperty("reconnect.backoff.multiplier"));
            MQTT_VERSION = properties.getProperty("mqtt.version");
            CLEAN_SESSION = Boolean.parseBoolean(properties.getProperty("clean.session"));
            CLIENT_ID = properties.getProperty("client.id");
            USER = properties.getProperty("userName");
            PASSWORD = properties.getProperty("password");
            String qos = properties.getProperty("mqtt.qos");
            switch (qos) {

                case "AT_LEAST_ONCE": {
                    MQTT_QOS = QoS.AT_LEAST_ONCE;
                    break;
                }
                case "AT_MOST_ONCE": {
                    MQTT_QOS = QoS.AT_MOST_ONCE;
                    break;
                }
                case "EXACTLY_ONCE": {
                    MQTT_QOS = QoS.EXACTLY_ONCE;
                    break;
                }
                default: {
                    MQTT_QOS = QoS.AT_LEAST_ONCE;
                }
            }

            String[] destinationNames = DESTINATIONS.split(",");

            List<String> destinations = new ArrayList<>();

            for (String destinationName : destinationNames) {
                LOG.debug("Getting name for destination " + destinationName);
                destinations.add(properties.getProperty(destinationName));
                LOG.debug("Got name for destination: " + properties.getProperty(destinationName));
            }

            List<Thread> threads = new ArrayList<>();

            int j = 0;

            for (String destination : destinations) {

                for (int i = 0; i < NUM_THREADS_PER_DESTINATION; i++) {
                    LOG.info("Creating thread for destination " + destination);
                    ConsumerThread consumerThread = new ConsumerThread(REMOTE_ADDRESS, USER, PASSWORD, destination, MQTT_VERSION, MQTT_QOS, KEEPALIVE, MAX_READ_RATE, RECEIVE_BUFFER_SIZE, MAX_CONNECT_ATTEMPTS, MAX_RECONNECT_ATTEMPTS, RECONNECT_DELAY, MAX_RECONNECT_DELAY, RECONNECT_BACKOFF_MULTIPLIER, CLEAN_SESSION, CLIENT_ID + "." + Integer.toString(j) + "-" + Integer.toString(i));
                    consumerThread.start();
                    threads.add(consumerThread);
                    if (THREAD_DELAY > 0) {
                        Thread.sleep(THREAD_DELAY);
                    }
                }
                j++;
            }
            for (Thread thread : threads) {
                thread.join();
            }

        } catch (Exception ex) {
            LOG.error("Caught Exception: ", ex);
        }
    }
}
