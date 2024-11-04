/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.fusebyexample.artemis.client.simple;

import java.util.Map;
import java.util.Random;
import java.util.logging.Level;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author dhawkins
 */
public class ProducerThread extends Thread implements MessageListener {

    private Session session = null;
    private Connection connection = null;
    private MessageProducer producer = null;
    private final ConnectionFactory factory;
    private final Destination destination;
    private final String clientPrefix;
    private final int iterations;
    private final int delay;
    private final int threadNum;
    private final int messageLength;
    private final int transactedBatchSize;
    private final long ttl;
    private final boolean messageLengthFixed;
    private final boolean transacted;
    private final boolean transactedBatch;
    private final boolean persistent;
    private final boolean dynamic;
    private final boolean replyTo;
    private boolean isDone = false;
    private final String messageBody;
    private final Map<String, String> additionalStringHeaders;
    private final Map<String, Integer> additionalIntHeaders;
    private final Map<String, Long> additionalLongHeaders;
    private final Map<String, Double> additionalDblHeaders;
    private final Map<String, Boolean> additionalBoolHeaders;
    private static final Logger LOG = LoggerFactory.getLogger(ProducerThread.class);

    public ProducerThread(ConnectionFactory factory, Destination destination, String clientPrefix, int iterations, int delay, long ttl, int threadNum, int messageLength, boolean messageLengthFixed, boolean transacted, boolean transactedBatch, int transactedBatchSize, boolean persistent, boolean dynamic, String messageBody, Map stringHeaders, Map intHeaders, Map longHeaders, Map dblHeaders, Map boolHeaders, boolean replyTo) {
        this.factory = factory;
        this.destination = destination;
        this.clientPrefix = clientPrefix;
        this.iterations = iterations;
        this.delay = delay;
        this.ttl = ttl;
        this.threadNum = threadNum;
        this.messageLength = messageLength;
        this.messageLengthFixed = messageLengthFixed;
        this.transactedBatchSize = transactedBatchSize;
        this.transacted = transacted;
        this.transactedBatch = transactedBatch;
        this.persistent = persistent;
        this.dynamic = dynamic;
        this.messageBody = messageBody;
        this.additionalStringHeaders = stringHeaders;
        this.additionalIntHeaders = intHeaders;
        this.additionalLongHeaders = longHeaders;
        this.additionalDblHeaders = dblHeaders;
        this.additionalBoolHeaders = boolHeaders;
        this.replyTo = replyTo;
    }

    @Override
    public void run() {

        while(true) {
        try {
            MessageConsumer responseConsumer = null;
            Destination tempDest = null;

            connection = factory.createConnection();
            connection.setClientID(clientPrefix + "." + destination + "-" + Integer.toString(threadNum));
            connection.start();
            if (transacted) {
                session = connection.createSession(transacted, Session.SESSION_TRANSACTED);
            } else {
                session = connection.createSession(transacted, Session.AUTO_ACKNOWLEDGE);
            }

            if (dynamic) {
                producer = session.createProducer(null);
            } else {
                producer = session.createProducer(destination);
            }
            producer.setTimeToLive(ttl);
            if (persistent) {
                producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            } else {
                producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            }

            if (replyTo) {
                tempDest = session.createTemporaryQueue();
                responseConsumer = session.createConsumer(tempDest);
                responseConsumer.setMessageListener(this);
            }

            String padding = null;

            if (messageBody == null && messageLengthFixed) {
                LOG.info("Building random message body");
                StringBuilder builder = new StringBuilder();
                Random random = new Random();
                for (int j = 0; j < messageLength; j++) {
                    builder.append((char) (random.nextInt(94) + 32));
                }
                padding = builder.toString();
            } else {
                LOG.info("Using message body: " + messageBody);
            }

            String groupId = "";

            LOG.info("Starting Production at: {} for {}", System.currentTimeMillis(), connection.getClientID());
            int count = 0;
            for (int i = 1; i <= iterations; i++) {

                count++;
                try {
                    TextMessage message = null;
                    if (messageBody != null) {
                        message = session.createTextMessage(messageBody);
                    } else {
                        message = session.createTextMessage("Sending message " + i + " at " + System.currentTimeMillis() + ": " + padding + ": to " + producer.getDestination() + " from " + threadNum);
                    }

                    if ((i == 0) || ((i > 0) && (i % 200 == 0))) {
                        groupId = this.clientPrefix + "-" + i;
                    }
                    message.setStringProperty("_AMQ_GROUP_ID", groupId);

                    if (replyTo) {
                        message.setJMSReplyTo(tempDest);
                    }

                    for (String key : additionalStringHeaders.keySet()) {
                        message.setStringProperty(key, additionalStringHeaders.get(key));
                    }

                    for (String key : additionalIntHeaders.keySet()) {
                        message.setIntProperty(key, additionalIntHeaders.get(key));
                    }

                    for (String key : additionalLongHeaders.keySet()) {
                        message.setLongProperty(key, additionalLongHeaders.get(key));
                    }

                    for (String key : additionalDblHeaders.keySet()) {
                        message.setDoubleProperty(key, additionalDblHeaders.get(key));
                    }

                    for (String key : additionalBoolHeaders.keySet()) {
                        message.setBooleanProperty(key, additionalBoolHeaders.get(key));
                    }

                    if (dynamic) {
                        producer.send(destination, message);
                    } else {
                        producer.send(message);
                    }
                    if ((transacted && count >= transactedBatchSize) || (transacted && transactedBatch == false)) {
                        session.commit();
                        count = 0;
                        LOG.info("{} : Thread {}: comitted session", System.currentTimeMillis(), threadNum);
                    }
                    LOG.info("{} : Thread {}: Sent {}. message: {}", System.currentTimeMillis(), threadNum, i, message.getJMSMessageID());

                } catch (JMSException ex) {
                    LOG.error("Caught JMSException: ", ex);
                    if (session != null) {
                        try {
                            session.close();
                        } catch (JMSException ex1) {
                            LOG.error("Caught JMSException: ", ex1);
                        }
                    }
                    if (connection != null) {
                        try {
                            connection.close();
                        } catch (JMSException ex2) {
                            LOG.error("Error closing connection", ex2);
                        }
                    }
                    connection = factory.createConnection();
                    connection.setClientID(clientPrefix + "." + destination + "-" + Integer.toString(threadNum));
                    connection.start();
                    if (transacted) {
                        session = connection.createSession(transacted, Session.SESSION_TRANSACTED);
                    } else {
                        session = connection.createSession(transacted, Session.AUTO_ACKNOWLEDGE);
                    }

                    if (dynamic) {
                        producer = session.createProducer(null);
                    } else {
                        producer = session.createProducer(destination);
                    }
                    producer.setTimeToLive(ttl);
                    if (persistent) {
                        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
                    } else {
                        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
                    }

                    if (replyTo) {
                        tempDest = session.createTemporaryQueue();
                        responseConsumer = session.createConsumer(tempDest);
                        responseConsumer.setMessageListener(this);
                    }
                    i--;
                }
                if (delay > 0) {
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException ex) {
                        isDone = true;
                    }
                }
            }
            if (transacted) {
                session.commit();
            }
            while (this.replyTo) {
                Thread.sleep(5000);
            }
            //producer.close();
            isDone = true;
        } catch (JMSException eJMS) {
            LOG.error("Caught JMSException: ", eJMS);
        } catch (InterruptedException ex) {
            java.util.logging.Logger.getLogger(ProducerThread.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            if (session != null) {
                try {
                    session.close();
                } catch (JMSException ex) {
                    LOG.error("Caught JMSException: ", ex);
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (JMSException e) {
                    LOG.error("Error closing connection", e);
                }
            }
        }
        }
    }

    @Override
    public void onMessage(Message message) {
        String messageText = null;
        try {
            if (message instanceof TextMessage) {
                TextMessage textMessage = (TextMessage) message;
                messageText = textMessage.getText();
                LOG.info("messageText = " + messageText);
            }
        } catch (JMSException e) {
            //Handle the exception appropriately
        }
    }

    public boolean isDone() {
        return isDone;
    }

}
