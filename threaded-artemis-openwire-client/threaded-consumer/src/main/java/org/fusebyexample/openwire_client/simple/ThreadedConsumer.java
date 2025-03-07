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
package org.fusebyexample.openwire_client.simple;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Properties;
import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import org.apache.activemq.ActiveMQConnectionFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThreadedConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(ThreadedConsumer.class);

    private static String INITIAL_CONTEXT_FACTORY = "org.apache.activemq.jndi.ActiveMQInitialContextFactory";
    private static String NAMING_PROVIDER_URL;
    private static final String DESTINATIONS = "destinations";
    private static String CONNECTION_FACTORY_NAME = "myJmsFactory";
    private static String CLIENT_PREFIX = "client";
    private static boolean SESSION_TRANSACTED = false;
    private static int MESSAGE_TIMEOUT_MILLISECONDS = 30000;
    private static int NUM_THREADS_PER_DESTINATION = 1;
    private static long THREAD_STARTUP_DELAY = 0;
    private static boolean TRANSACTION_IS_BATCH = false;
    private static long TRANSACTION_DELAY = 0;
    private static long READ_DELAY = 0;
    private static boolean ACK_ASYNC = true;
    private static boolean COPY_ON_SEND = false;
    private static boolean WATCH_TOPIC_ADVISIORIES = false;
    private static boolean IS_DURABLE_SUBSCRIBER = false;
    private static boolean EXCLUSIVE_CONSUMER = false;
    private static boolean STATS_ENABLED = false;
    private static boolean OPTIMIZE_ACK = false;
    private static boolean BROWSE_ONLY = false;
    private static boolean UNIQUE_CLIENT_ID = false;
    private static boolean THROW_EXCEPTION = false;
    private static int CONNECTION_CLOSE_TIMEOUT = 15000;
    private static boolean IS_EXIT_AFTER_CONNECTING = false;
    private static String SELECTOR;

    public static void main(String args[]) throws InterruptedException, IOException {

        String brokerUrl = System.getProperty("java.naming.provider.url");

        if (brokerUrl != null) {
            LOG.info("******************************");
            LOG.info("Overriding jndi brokerUrl, now using: {}", brokerUrl);
            LOG.info("******************************");
        }

        try {

            Properties properties = new Properties();
            try {
                properties.load(new FileInputStream("jndi.properties"));
            } catch (IOException ex) {
                LOG.warn("jndi.properties not found, defaulting to packaged version.");
                properties.load(ThreadedConsumer.class.getResourceAsStream("/jndi.properties"));
            }

            INITIAL_CONTEXT_FACTORY = properties.getProperty("java.naming.factory.initial");
            NAMING_PROVIDER_URL = properties.getProperty("java.naming.provider.url");
            CONNECTION_FACTORY_NAME = properties.getProperty("connectionFactoryNames");
            CLIENT_PREFIX = properties.getProperty("client.prefix");
            SESSION_TRANSACTED = Boolean.parseBoolean(properties.getProperty("session.transacted"));
            MESSAGE_TIMEOUT_MILLISECONDS = Integer.parseInt(properties.getProperty("message.timeout.ms"));
            NUM_THREADS_PER_DESTINATION = Integer.parseInt(properties.getProperty("num.threads.per.dest"));
            THREAD_STARTUP_DELAY = Long.parseLong(properties.getProperty("delay.between.threads"));
            TRANSACTION_IS_BATCH = Boolean.parseBoolean(properties.getProperty("transacted.batch"));
            TRANSACTION_DELAY = Long.parseLong(properties.getProperty("transacted.delay"));
            READ_DELAY = Long.parseLong(properties.getProperty("read.delay"));
            ACK_ASYNC = Boolean.parseBoolean(properties.getProperty("ack.async"));
            COPY_ON_SEND = Boolean.parseBoolean(properties.getProperty("copy.on.send"));
            WATCH_TOPIC_ADVISIORIES = Boolean.parseBoolean(properties.getProperty("watch.topic.advisories"));
            IS_DURABLE_SUBSCRIBER = Boolean.parseBoolean(properties.getProperty("is.durable.subscriber"));
            EXCLUSIVE_CONSUMER = Boolean.parseBoolean(properties.getProperty("exclusive.consumer"));
            STATS_ENABLED = Boolean.parseBoolean(properties.getProperty("stats.enabled"));
            OPTIMIZE_ACK = Boolean.parseBoolean(properties.getProperty("optimize.ack"));
            BROWSE_ONLY = Boolean.parseBoolean(properties.getProperty("is.browser"));
            UNIQUE_CLIENT_ID = Boolean.parseBoolean(properties.getProperty("use.unique.clientid"));
            THROW_EXCEPTION = Boolean.parseBoolean(properties.getProperty("is.throw.exception"));
            IS_EXIT_AFTER_CONNECTING = Boolean.parseBoolean(properties.getProperty("is.exit.after.connecting"));
            CONNECTION_CLOSE_TIMEOUT = Integer.parseInt(properties.getProperty("connection.close.timeout"));
            SELECTOR = properties.getProperty("message.selector");

            System.setProperty("java.naming.factory.initial", INITIAL_CONTEXT_FACTORY);
            System.setProperty("java.naming.provider.url", NAMING_PROVIDER_URL);

            String destinationNameList = properties.getProperty(DESTINATIONS);
            String[] destinationNames = destinationNameList.split(",");

            // JNDI lookup of JMS Connection Factory and JMS Destination
            Hashtable env = new Hashtable();
            for (String property : properties.stringPropertyNames()) {
                env.put(property, properties.getProperty(property));
            }

            Context context = new InitialContext(env);
            ConnectionFactory factory = (ConnectionFactory) context.lookup(CONNECTION_FACTORY_NAME);

            ((ActiveMQConnectionFactory) factory).setSendAcksAsync(ACK_ASYNC);
            ((ActiveMQConnectionFactory) factory).setCopyMessageOnSend(COPY_ON_SEND);
            ((ActiveMQConnectionFactory) factory).setWatchTopicAdvisories(WATCH_TOPIC_ADVISIORIES);
            ((ActiveMQConnectionFactory) factory).setExclusiveConsumer(EXCLUSIVE_CONSUMER);
            ((ActiveMQConnectionFactory) factory).setStatsEnabled(STATS_ENABLED);
            ((ActiveMQConnectionFactory) factory).setCloseTimeout(CONNECTION_CLOSE_TIMEOUT);
            ((ActiveMQConnectionFactory) factory).setCloseTimeout(CONNECTION_CLOSE_TIMEOUT);
            ((ActiveMQConnectionFactory) factory).setOptimizeAcknowledge(OPTIMIZE_ACK);

            List<Destination> destinations = new ArrayList<>();

            for (String destinationName : destinationNames) {
                destinations.add((Destination) context.lookup(destinationName));
            }

            List<Thread> threads = new ArrayList<>();

            for (Destination destination : destinations) {

                for (int i = 0; i < NUM_THREADS_PER_DESTINATION; i++) {
                    if (!BROWSE_ONLY) {
                        ConsumerThread consumerThread = null;
                        if (i % 2 == 0) {
                            consumerThread = new ConsumerThread(factory, destination, i + 1, CLIENT_PREFIX, MESSAGE_TIMEOUT_MILLISECONDS, SELECTOR, SESSION_TRANSACTED, TRANSACTION_IS_BATCH, TRANSACTION_DELAY, 0, UNIQUE_CLIENT_ID, THROW_EXCEPTION, IS_DURABLE_SUBSCRIBER);
                        } else {
                            consumerThread = new ConsumerThread(factory, destination, i + 1, CLIENT_PREFIX, MESSAGE_TIMEOUT_MILLISECONDS, SELECTOR, SESSION_TRANSACTED, TRANSACTION_IS_BATCH, TRANSACTION_DELAY, READ_DELAY, UNIQUE_CLIENT_ID, THROW_EXCEPTION, IS_DURABLE_SUBSCRIBER);
                        }
                        consumerThread.start();
                        threads.add(consumerThread);
                    } else {
                        BrowserThread browserThread = new BrowserThread(factory, destination, i + 1, CLIENT_PREFIX, MESSAGE_TIMEOUT_MILLISECONDS, SELECTOR, SESSION_TRANSACTED, TRANSACTION_IS_BATCH, TRANSACTION_DELAY, READ_DELAY, UNIQUE_CLIENT_ID);
                        browserThread.start();
                        threads.add(browserThread);
                    }
                    if (THREAD_STARTUP_DELAY > 0) {
                        Thread.sleep(THREAD_STARTUP_DELAY);
                    }
                }
            }

            if (IS_EXIT_AFTER_CONNECTING) {
                Thread.sleep(30000);
                System.exit(1);
            } else {
                for (Thread thread : threads) {
                    thread.join();
                }
            }

        } catch (NamingException eN) {
            LOG.error("Caught NamingException: ", eN);
        }
    }
}
