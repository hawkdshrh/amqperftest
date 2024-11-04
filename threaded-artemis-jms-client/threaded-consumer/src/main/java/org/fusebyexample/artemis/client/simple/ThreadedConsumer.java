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
package org.fusebyexample.artemis.client.simple;

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
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThreadedConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(ThreadedConsumer.class);

    private static String INITIAL_CONTEXT_FACTORY = "org.apache.activemq.jndi.ActiveMQInitialContextFactory";
    private static String NAMING_PROVIDER_URL;
    private static final String DESTINATIONS = "destinations";
    private static String CONNECTION_FACTORY_NAME = "myJmsFactory";
    private static String CLIENT_PREFIX = "client";
    private static int CONSUMER_MAX_RATE = -1;
    private static int CONFIRMATION_WINDOW_SIZE = 1000000;
    private static int CONSUMER_WINDOW_SIZE = 1000000;
    private static int MESSAGE_TIMEOUT_MILLISECONDS = 30000;
    private static int NUM_THREADS_PER_DESTINATION = 1;
    private static int MESSAGE_COUNT_PER_DESTINATION = -1;
    private static long CALL_FAILOVER_TIMEOUT = -1;
    private static long CALL_TIMEOUT = 30000;
    private static long CONNECTION_TTL = 600000;
    private static long READ_DELAY = 0;
    private static long THREAD_STARTUP_DELAY = 0;
    private static long TRANSACTION_DELAY = 0;
    private static boolean AUTO_GROUP = true;
    private static boolean BLOCK_ON_ACKNOWLEDGE = false;
    private static boolean BLOCK_ON_DURABLE_SEND = false;
    private static boolean BLOCK_ON_NONDURABLE_SEND = false;
    private static boolean CACHE_DESTINATIONS = false;
    private static boolean CACHE_LARGE_MESSAGES = false;
    private static boolean COMPRESS_LARGE_MESSAGES = false;
    private static boolean IS_DURABLE_SUBSCRIBER = false;
    private static boolean SESSION_TRANSACTED = false;
    private static boolean TRANSACTION_IS_BATCH = false;
    private static boolean UNIQUE_CLIENT_ID = false;
    private static boolean THROW_EXCEPTION = false;
    private static String SELECTOR;
    private static String USERNAME;
    private static String PASSWORD;

    public static void main(String args[]) throws InterruptedException, IOException, JMSException {

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
            USERNAME = properties.getProperty("userName");
            PASSWORD = properties.getProperty("password");
            CLIENT_PREFIX = properties.getProperty("client.prefix");
            CONFIRMATION_WINDOW_SIZE = Integer.parseInt(properties.getProperty("confirmation.window.size"));
            CONSUMER_MAX_RATE = Integer.parseInt(properties.getProperty("consumer.max.rate"));
            CONSUMER_WINDOW_SIZE = Integer.parseInt(properties.getProperty("consumer.window.size"));
            MESSAGE_TIMEOUT_MILLISECONDS = Integer.parseInt(properties.getProperty("message.timeout.ms"));
            NUM_THREADS_PER_DESTINATION = Integer.parseInt(properties.getProperty("num.threads.per.dest"));
            MESSAGE_COUNT_PER_DESTINATION = Integer.parseInt(properties.getProperty("message.count.per.dest"));
            CALL_FAILOVER_TIMEOUT = Long.parseLong(properties.getProperty("call.failover.timeout"));
            CALL_TIMEOUT = Long.parseLong(properties.getProperty("call.timeout"));
            CONNECTION_TTL = Long.parseLong(properties.getProperty("connection.ttl"));
            READ_DELAY = Long.parseLong(properties.getProperty("read.delay"));
            THREAD_STARTUP_DELAY = Long.parseLong(properties.getProperty("delay.between.threads"));
            TRANSACTION_DELAY = Long.parseLong(properties.getProperty("transacted.delay"));
            AUTO_GROUP = Boolean.parseBoolean(properties.getProperty("auto.group"));
            BLOCK_ON_ACKNOWLEDGE = Boolean.parseBoolean(properties.getProperty("block.on.acknowledge"));
            BLOCK_ON_DURABLE_SEND = Boolean.parseBoolean(properties.getProperty("block.on.durable.send"));
            BLOCK_ON_NONDURABLE_SEND = Boolean.parseBoolean(properties.getProperty("block.on.nondurable.send"));
            CACHE_DESTINATIONS = Boolean.parseBoolean(properties.getProperty("cache.destinations"));
            CACHE_LARGE_MESSAGES = Boolean.parseBoolean(properties.getProperty("cache.large.messages"));
            COMPRESS_LARGE_MESSAGES = Boolean.parseBoolean(properties.getProperty("compress.large.messages"));
            IS_DURABLE_SUBSCRIBER = Boolean.parseBoolean(properties.getProperty("is.durable.subscriber"));
            SESSION_TRANSACTED = Boolean.parseBoolean(properties.getProperty("session.transacted"));
            TRANSACTION_IS_BATCH = Boolean.parseBoolean(properties.getProperty("transacted.batch"));
            THROW_EXCEPTION = Boolean.parseBoolean(properties.getProperty("is.throw.exception"));
            UNIQUE_CLIENT_ID = Boolean.parseBoolean(properties.getProperty("use.unique.clientid"));

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

            ((ActiveMQConnectionFactory) factory).setUser(USERNAME);
            ((ActiveMQConnectionFactory) factory).setPassword(PASSWORD);
            ((ActiveMQConnectionFactory) factory).setAutoGroup(AUTO_GROUP);
            ((ActiveMQConnectionFactory) factory).setBlockOnAcknowledge(BLOCK_ON_ACKNOWLEDGE);
            ((ActiveMQConnectionFactory) factory).setBlockOnDurableSend(BLOCK_ON_DURABLE_SEND);
            ((ActiveMQConnectionFactory) factory).setBlockOnNonDurableSend(BLOCK_ON_NONDURABLE_SEND);
            ((ActiveMQConnectionFactory) factory).setCacheDestinations(CACHE_DESTINATIONS);
            ((ActiveMQConnectionFactory) factory).setCacheLargeMessagesClient(CACHE_LARGE_MESSAGES);
            ((ActiveMQConnectionFactory) factory).setCallFailoverTimeout(CALL_FAILOVER_TIMEOUT);
            ((ActiveMQConnectionFactory) factory).setCallTimeout(CALL_TIMEOUT);
            //((ActiveMQConnectionFactory) factory).setClientFailureCheckPeriod(30000);
            ((ActiveMQConnectionFactory) factory).setCompressLargeMessage(COMPRESS_LARGE_MESSAGES);
            ((ActiveMQConnectionFactory) factory).setConfirmationWindowSize(CONFIRMATION_WINDOW_SIZE);
            ((ActiveMQConnectionFactory) factory).setConnectionTTL(CONNECTION_TTL);
            ((ActiveMQConnectionFactory) factory).setConsumerMaxRate(CONSUMER_MAX_RATE);
            ((ActiveMQConnectionFactory) factory).setConsumerWindowSize(CONSUMER_WINDOW_SIZE);

            List<Destination> destinations = new ArrayList<>();

            for (String destinationName : destinationNames) {
                destinations.add((Destination) context.lookup(destinationName));
            }

            List<Thread> threads = new ArrayList<>();

            for (Destination destination : destinations) {

                for (int i = 0; i < NUM_THREADS_PER_DESTINATION; i++) {

                    ConsumerThread consumerThread = new ConsumerThread(factory, destination, i + 1, CLIENT_PREFIX, MESSAGE_TIMEOUT_MILLISECONDS, SELECTOR, SESSION_TRANSACTED, TRANSACTION_IS_BATCH, TRANSACTION_DELAY, READ_DELAY, UNIQUE_CLIENT_ID, THROW_EXCEPTION, IS_DURABLE_SUBSCRIBER, MESSAGE_COUNT_PER_DESTINATION / NUM_THREADS_PER_DESTINATION);
                    consumerThread.start();
                    threads.add(consumerThread);

                    if (THREAD_STARTUP_DELAY > 0) {
                        Thread.sleep(THREAD_STARTUP_DELAY);
                    }
                }
            }
            for (Thread thread : threads) {
                thread.join();
            }

        } catch (NamingException eN) {
            LOG.error("Caught NamingException: ", eN);
        }
    }
}
