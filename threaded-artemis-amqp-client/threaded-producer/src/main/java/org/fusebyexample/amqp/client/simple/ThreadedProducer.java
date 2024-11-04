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
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import org.apache.qpid.jms.JmsConnectionFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThreadedProducer {

    private static final Logger LOG = LoggerFactory.getLogger(ThreadedProducer.class);

    private static String INITIAL_CONTEXT_FACTORY = "org.apache.qpid.jms.jndi.JmsInitialContextFactory";
    private static int MESSAGE_DELAY_MILLISECONDS = 1;
    private static int MESSAGE_LENGTH = 0;
    private static int NUM_MESSAGES_TO_BE_SENT_PER_DESTINATION = 1;
    private static int NUM_THREADS_PER_DESTINATION = 1;  
    private static int GENERATE_STRING_HEADER_SIZE = 0;
    
    private static long MESSAGE_TIME_TO_LIVE_MILLISECONDS = 0;
    private static long THREAD_STARTUP_DELAY = 0;
    private static long TRANSACTED_DELAY = 0;
    
    private static boolean TRANSACTED = false;
    private static boolean PERSISTENT = true;
    private static boolean DYNAMIC = false;
    private static boolean REPLYTO = false;
    private static boolean TEST_PRIORITY = false;
    private static boolean GENERATE_STRING_HEADER = false;
 
    private static String BODY = null;    
    private static String BROKER_URL;    
    private static String CLIENT_PREFIX = "client";    
    private static String USERNAME;
    private static String PASSWORD;
    private static String GENERATE_STRING_HEADER_NAME = null;

    private static final String DESTINATIONS = "destinations";
    private static final String STRING_HEADERNAMES = "headers.string";
    private static final String INT_HEADERNAMES = "headers.integer";
    private static final String LONG_HEADERNAMES = "headers.long";
    private static final String DBL_HEADERNAMES = "headers.double";
    private static final String BOOL_HEADERNAMES = "headers.boolean";
    
    private static final Map<String, String> STRING_HEADERS = new HashMap<>();
    private static final Map<String, Integer> INT_HEADERS = new HashMap<>();
    private static final Map<String, Long> LONG_HEADERS = new HashMap<>();
    private static final Map<String, Double> DBL_HEADERS = new HashMap<>();
    private static final Map<String, Boolean> BOOL_HEADERS = new HashMap<>();


    public static void main(String args[]) throws IOException, InterruptedException {

        try {

            Properties properties = new Properties();
            try {
                properties.load(new FileInputStream("jndi.properties"));
            } catch (IOException ex) {
                LOG.warn("jndi.properties not found, defaulting to packaged version.");
                properties.load(ThreadedProducer.class.getResourceAsStream("/jndi.properties"));
            }

            INITIAL_CONTEXT_FACTORY = properties.getProperty("java.naming.factory.initial");
            BROKER_URL = properties.getProperty("broker.url");
            USERNAME = properties.getProperty("userName");
            PASSWORD = properties.getProperty("password");
            GENERATE_STRING_HEADER_NAME = properties.getProperty("generate.string.header.name");
            MESSAGE_DELAY_MILLISECONDS = Integer.parseInt(properties.getProperty("message.delay.ms"));
            MESSAGE_TIME_TO_LIVE_MILLISECONDS = Integer.parseInt(properties.getProperty("message.ttl.ms"));
            NUM_MESSAGES_TO_BE_SENT_PER_DESTINATION = Integer.parseInt(properties.getProperty("num.messages.per.dest"));
            NUM_THREADS_PER_DESTINATION = Integer.parseInt(properties.getProperty("num.threads.per.dest"));
            GENERATE_STRING_HEADER_SIZE = Integer.parseInt(properties.getProperty("generate.string.header.size"));
            THREAD_STARTUP_DELAY = Long.parseLong(properties.getProperty("delay.between.threads"));
            TRANSACTED_DELAY = Long.parseLong(properties.getProperty("transacted.delay"));
            MESSAGE_LENGTH = Integer.parseInt(properties.getProperty("message.length"));
            CLIENT_PREFIX = properties.getProperty("client.prefix");
            TRANSACTED = Boolean.parseBoolean(properties.getProperty("transacted"));
            PERSISTENT = Boolean.parseBoolean(properties.getProperty("persistent"));
            DYNAMIC = Boolean.parseBoolean(properties.getProperty("dynamic"));
            REPLYTO = Boolean.parseBoolean(properties.getProperty("replyto"));
            TEST_PRIORITY = Boolean.parseBoolean(properties.getProperty("testpriority"));
            GENERATE_STRING_HEADER = Boolean.parseBoolean(properties.getProperty("generate.string.header"));
            BODY = properties.getProperty("body");

            System.setProperty("java.naming.factory.initial", INITIAL_CONTEXT_FACTORY);

            String destinationNameList = properties.getProperty(DESTINATIONS);
            String[] destinationNames = destinationNameList.split(",");

            String stringHeaderNameList = properties.getProperty(STRING_HEADERNAMES);
            String[] stringHeaderNames = null;
            if (stringHeaderNameList != null && !stringHeaderNameList.isEmpty()) {
                stringHeaderNames = stringHeaderNameList.split(",");
            }

            String intHeaderNameList = properties.getProperty(INT_HEADERNAMES);
            String[] intHeaderNames = null;
            if (intHeaderNameList != null && !intHeaderNameList.isEmpty()) {
                intHeaderNames = intHeaderNameList.split(",");
            }

            String longHeaderNameList = properties.getProperty(LONG_HEADERNAMES);
            String[] longHeaderNames = null;
            if (longHeaderNameList != null && !longHeaderNameList.isEmpty()) {
                longHeaderNames = longHeaderNameList.split(",");
            }

            String dblHeaderNameList = properties.getProperty(DBL_HEADERNAMES);
            String[] dblHeaderNames = null;
            if (dblHeaderNameList != null && !dblHeaderNameList.isEmpty()) {
                dblHeaderNames = dblHeaderNameList.split(",");
            }

            String boolHeaderNameList = properties.getProperty(BOOL_HEADERNAMES);
            String[] boolHeaderNames = null;
            if (boolHeaderNameList != null && !boolHeaderNameList.isEmpty()) {
                boolHeaderNames = boolHeaderNameList.split(",");
            }

            // JNDI lookup of JMS Connection Factory and JMS Destination
            Hashtable env = new Hashtable();
            for (String property : properties.stringPropertyNames()) {
                env.put(property, properties.getProperty(property));
            }

            Context context = new InitialContext(env);
            JmsConnectionFactory factory = new JmsConnectionFactory();
            factory.setUsername(USERNAME);
            factory.setPassword(PASSWORD);
            factory.setRemoteURI(BROKER_URL);

            List<Destination> destinations = new ArrayList<>();

            for (String destinationName : destinationNames) {
                destinations.add((Destination) context.lookup(destinationName));
            }

            if (stringHeaderNames != null) {
                for (String stringHeaderName : stringHeaderNames) {
                    STRING_HEADERS.put(stringHeaderName, properties.getProperty(stringHeaderName));
                }
            }

            if (intHeaderNames != null) {
                for (String intHeaderName : intHeaderNames) {
                    INT_HEADERS.put(intHeaderName, Integer.parseInt(properties.getProperty(intHeaderName)));
                }
            }

            if (longHeaderNames != null) {
                for (String longHeaderName : longHeaderNames) {
                    LONG_HEADERS.put(longHeaderName, Long.parseLong(properties.getProperty(longHeaderName)));
                }
            }

            if (dblHeaderNames != null) {
                for (String dblHeaderName : dblHeaderNames) {
                    DBL_HEADERS.put(dblHeaderName, Double.parseDouble(properties.getProperty(dblHeaderName)));
                }
            }

            if (boolHeaderNames != null) {
                for (String boolHeaderName : boolHeaderNames) {
                    BOOL_HEADERS.put(boolHeaderName, Boolean.parseBoolean(properties.getProperty(boolHeaderName)));
                }
            }

            List<ProducerThread> threads = new ArrayList<>();

            for (Destination destination : destinations) {

                for (int i = 0; i < NUM_THREADS_PER_DESTINATION; i++) {

                    ProducerThread producerThread = new ProducerThread(factory, destination, CLIENT_PREFIX, NUM_MESSAGES_TO_BE_SENT_PER_DESTINATION / NUM_THREADS_PER_DESTINATION, MESSAGE_DELAY_MILLISECONDS, MESSAGE_TIME_TO_LIVE_MILLISECONDS, i + 1, MESSAGE_LENGTH, TRANSACTED, TRANSACTED_DELAY, PERSISTENT, DYNAMIC, BODY, STRING_HEADERS, INT_HEADERS, LONG_HEADERS, DBL_HEADERS, BOOL_HEADERS, REPLYTO, TEST_PRIORITY, GENERATE_STRING_HEADER, GENERATE_STRING_HEADER_NAME, GENERATE_STRING_HEADER_SIZE);
                    producerThread.start();
                    threads.add(producerThread);

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
