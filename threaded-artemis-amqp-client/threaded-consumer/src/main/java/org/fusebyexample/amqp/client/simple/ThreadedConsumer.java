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
package org.fusebyexample.amqp.client.simple;

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
import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.policy.JmsDefaultMessageIDPolicy;
import org.apache.qpid.jms.policy.JmsDefaultPrefetchPolicy;
import org.apache.qpid.jms.policy.JmsDefaultPresettlePolicy;
import org.apache.qpid.jms.policy.JmsDefaultRedeliveryPolicy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThreadedConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(ThreadedConsumer.class);

    private static String INITIAL_CONTEXT_FACTORY = "org.apache.qpid.jms.jndi.JmsInitialContextFactory";
    private static String BROKER_URL;
    private static final String DESTINATIONS = "destinations";
    private static String CONNECTION_FACTORY_NAME = "myJmsFactory";
    private static String CLIENT_PREFIX = "client";
    private static String CLIENT_ID = "";

    private static int MESSAGE_TIMEOUT_MILLISECONDS = 30000;
    private static int NUM_THREADS_PER_DESTINATION = 1;

    private static long CLOSE_TIMEOUT = 30000;
    private static long CONNECT_TIMEOUT = 30000;
    private static long READ_DELAY = 0;
    private static long REQUEST_TIMEOUT = 30000;
    private static long THREAD_STARTUP_DELAY = 0;
    private static long TRANSACTION_DELAY = 0;

    private static boolean AWAIT_CLIENT_ID = false;
    private static boolean CLOSE_LINKS_THAT_FAIL_ON_RECONNECT = true;
    private static boolean FORCE_ASYNC_ACKS = false;
    private static boolean FORCE_ASYNC_SEND = false;
    private static boolean FORCE_SYNC_SEND = false;
    private static boolean IS_DURABLE_SUBSCRIBER = false;
    private static boolean IS_NO_ACK = false;
    private static boolean POPULATE_JMSX_USER_ID = false;
    private static boolean LOCAL_MESSAGE_EXPIRY = true;
    private static boolean LOCAL_MESSAGE_PRIORITY = true;
    private static boolean RECEIVE_LOCAL_ONLY = true;
    private static boolean RECEIVE_LOCAL_ONLY_NO_WAIT = true;
    private static boolean SESSION_TRANSACTED = false;
    private static boolean TRANSACTION_IS_BATCH = false;
    private static boolean THROW_EXCEPTION = false;
    private static boolean UNIQUE_CLIENT_ID = false;

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
            BROKER_URL = properties.getProperty("broker.url");
            CONNECTION_FACTORY_NAME = properties.getProperty("connectionFactoryNames");
            USERNAME = properties.getProperty("userName");
            PASSWORD = properties.getProperty("password");
            CLIENT_PREFIX = properties.getProperty("client.prefix");
            CLIENT_ID = properties.getProperty("client.id");

            CLOSE_LINKS_THAT_FAIL_ON_RECONNECT = Boolean.parseBoolean(properties.getProperty("close.links.that.fail.on.reconnect"));

            MESSAGE_TIMEOUT_MILLISECONDS = Integer.parseInt(properties.getProperty("message.timeout.ms"));
            NUM_THREADS_PER_DESTINATION = Integer.parseInt(properties.getProperty("num.threads.per.dest"));
            CLOSE_TIMEOUT = Long.parseLong(properties.getProperty("close.timeout"));
            CONNECT_TIMEOUT = Long.parseLong(properties.getProperty("connect.timeout"));
            READ_DELAY = Long.parseLong(properties.getProperty("read.delay"));
            REQUEST_TIMEOUT = Long.parseLong(properties.getProperty("request.timeout"));
            THREAD_STARTUP_DELAY = Long.parseLong(properties.getProperty("delay.between.threads"));
            TRANSACTION_DELAY = Long.parseLong(properties.getProperty("transacted.delay"));

            AWAIT_CLIENT_ID = Boolean.parseBoolean(properties.getProperty("await.clientid"));
            FORCE_ASYNC_ACKS = Boolean.parseBoolean(properties.getProperty("force.async.acks"));
            FORCE_ASYNC_SEND = Boolean.parseBoolean(properties.getProperty("force.async.send"));
            FORCE_SYNC_SEND = Boolean.parseBoolean(properties.getProperty("force.sync.send"));
            IS_DURABLE_SUBSCRIBER = Boolean.parseBoolean(properties.getProperty("is.durable.subscriber"));
            IS_NO_ACK = Boolean.parseBoolean(properties.getProperty("is.no.ack"));
            LOCAL_MESSAGE_EXPIRY = Boolean.parseBoolean(properties.getProperty("local.message.expiry"));
            LOCAL_MESSAGE_PRIORITY = Boolean.parseBoolean(properties.getProperty("local.message.priority"));
            RECEIVE_LOCAL_ONLY = Boolean.parseBoolean(properties.getProperty("receive.local.only"));
            RECEIVE_LOCAL_ONLY_NO_WAIT = Boolean.parseBoolean(properties.getProperty("receive.local.only.no.wait"));
            POPULATE_JMSX_USER_ID = Boolean.parseBoolean(properties.getProperty("populate.jmsx.user.id"));
            SESSION_TRANSACTED = Boolean.parseBoolean(properties.getProperty("session.transacted"));
            TRANSACTION_IS_BATCH = Boolean.parseBoolean(properties.getProperty("transacted.batch"));
            THROW_EXCEPTION = Boolean.parseBoolean(properties.getProperty("is.throw.exception"));
            UNIQUE_CLIENT_ID = Boolean.parseBoolean(properties.getProperty("unique.clientid"));

            SELECTOR = properties.getProperty("message.selector");

            System.setProperty("java.naming.factory.initial", INITIAL_CONTEXT_FACTORY);

            String destinationNameList = properties.getProperty(DESTINATIONS);
            String[] destinationNames = destinationNameList.split(",");

            // JNDI lookup of JMS Connection Factory and JMS Destination
            Hashtable env = new Hashtable();
            for (String property : properties.stringPropertyNames()) {
                env.put(property, properties.getProperty(property));
            }

            Context context = new InitialContext(env);
            JmsConnectionFactory factory = new JmsConnectionFactory();

            ((JmsConnectionFactory) factory).setUsername(USERNAME);
            ((JmsConnectionFactory) factory).setPassword(PASSWORD);
            ((JmsConnectionFactory) factory).setRemoteURI(BROKER_URL);

            ((JmsConnectionFactory) factory).setAwaitClientID(AWAIT_CLIENT_ID);
            if (CLIENT_ID != null && !CLIENT_ID.isEmpty()) {
                ((JmsConnectionFactory) factory).setClientID(CLIENT_ID);
            }
            if (CLIENT_PREFIX != null && !CLIENT_PREFIX.isEmpty()) {
                ((JmsConnectionFactory) factory).setClientIDPrefix(CLIENT_PREFIX);
            }
            ((JmsConnectionFactory) factory).setCloseLinksThatFailOnReconnect(CLOSE_LINKS_THAT_FAIL_ON_RECONNECT);
            ((JmsConnectionFactory) factory).setCloseTimeout(CLOSE_TIMEOUT);
            ((JmsConnectionFactory) factory).setConnectTimeout(CONNECT_TIMEOUT);
            ((JmsConnectionFactory) factory).setConnectionIDPrefix(CLIENT_PREFIX);
            //TODO
            //((JmsConnectionFactory) factory).setExceptionListener(exceptionListener);
            ((JmsConnectionFactory) factory).setForceAsyncAcks(FORCE_ASYNC_ACKS);
            ((JmsConnectionFactory) factory).setForceAsyncSend(FORCE_ASYNC_SEND);
            ((JmsConnectionFactory) factory).setForceSyncSend(FORCE_SYNC_SEND);
            ((JmsConnectionFactory) factory).setLocalMessageExpiry(LOCAL_MESSAGE_EXPIRY);
            ((JmsConnectionFactory) factory).setLocalMessagePriority(LOCAL_MESSAGE_PRIORITY);
            //TODO
            ((JmsConnectionFactory) factory).setMessageIDPolicy(new JmsDefaultMessageIDPolicy());
            ((JmsConnectionFactory) factory).setPopulateJMSXUserID(POPULATE_JMSX_USER_ID);
            //TODO
            ((JmsConnectionFactory) factory).setPrefetchPolicy(new JmsDefaultPrefetchPolicy());
            //TODO
            ((JmsConnectionFactory) factory).setPresettlePolicy(new JmsDefaultPresettlePolicy());

            ((JmsConnectionFactory) factory).setReceiveLocalOnly(RECEIVE_LOCAL_ONLY);
            ((JmsConnectionFactory) factory).setReceiveNoWaitLocalOnly(RECEIVE_LOCAL_ONLY_NO_WAIT);
            //TODO
            ((JmsConnectionFactory) factory).setRedeliveryPolicy(new JmsDefaultRedeliveryPolicy());
            ((JmsConnectionFactory) factory).setRemoteURI(BROKER_URL);
            ((JmsConnectionFactory) factory).setRequestTimeout(REQUEST_TIMEOUT);

            List<Destination> destinations = new ArrayList<>();

            for (String destinationName : destinationNames) {
                destinations.add((Destination)context.lookup(destinationName));
            }

            List<Thread> threads = new ArrayList<>();

            for (Destination destination : destinations) {

                for (int i = 0; i < NUM_THREADS_PER_DESTINATION; i++) {
                    ConsumerThread consumerThread = new ConsumerThread(factory, destination, i + 1, CLIENT_PREFIX, MESSAGE_TIMEOUT_MILLISECONDS, SELECTOR, SESSION_TRANSACTED, TRANSACTION_IS_BATCH, TRANSACTION_DELAY, READ_DELAY, UNIQUE_CLIENT_ID, THROW_EXCEPTION, IS_DURABLE_SUBSCRIBER, IS_NO_ACK);
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
