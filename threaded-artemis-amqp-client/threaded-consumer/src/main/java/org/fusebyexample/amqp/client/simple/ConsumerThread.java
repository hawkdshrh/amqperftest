/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.fusebyexample.amqp.client.simple;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TransactionRolledBackException;
import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.message.JmsMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author dhawkins
 */
public class ConsumerThread extends Thread {

    private Session session = null;
    private Session replySession = null;
    private Boolean transacted = false;
    private Connection connection = null;
    private final String selector;
    private MessageConsumer consumer = null;
    private final int threadNum;
    private final String clientPrefix;
    private final ConnectionFactory factory;
    private final Destination destination;
    private final int messageTimeoutMs;
    private final boolean transactionIsBatch;
    private final boolean uniqueClientId;
    private final long transactionDelay;
    private final long readDelay;
    private final Timer timer = new Timer();
    private final Executor executor = Executors.newCachedThreadPool();
    private Boolean isCommit = false;
    private Boolean isThrowException = false;
    private Boolean isDurableSub = false;
    private Boolean isNoAck = false;

    private boolean connected = false;
    private boolean isDone = false;

    private static final Logger LOG = LoggerFactory.getLogger(ConsumerThread.class);

    public ConsumerThread(ConnectionFactory factory, Destination destination, int threadNum, String clientPrefix, int messageTimeoutMs, String selector, boolean transacted, boolean transactionIsBatch, long transactionDelay, long readDelay, boolean uniqueClientId, boolean throwException, boolean isDurableSubscriber, boolean isNoAck) {
        this.factory = factory;
        this.destination = destination;
        this.selector = selector;
        this.threadNum = threadNum;
        this.clientPrefix = clientPrefix;
        this.transacted = transacted;
        this.messageTimeoutMs = messageTimeoutMs;
        this.transactionIsBatch = transactionIsBatch;
        this.transactionDelay = transactionDelay;
        this.readDelay = readDelay;
        this.uniqueClientId = uniqueClientId;
        this.isThrowException = throwException;
        this.isDurableSub = isDurableSubscriber;
        this.isNoAck = isNoAck;
    }

    @Override
    public void run() {

        int msgsRecd = 0;

        try {

            while (!isDone) {

                while (!connected && !isDone) {

                    try {
                        if (uniqueClientId) {
                            ((org.apache.qpid.jms.JmsConnectionFactory) factory).setClientID(clientPrefix + "." + destination + "-" + Integer.toString(this.threadNum) + "-" + Long.toString(System.currentTimeMillis()));
                        } 
                        
                        connection = factory.createConnection();

                        connection.start();
                        LOG.info("Started connection: " + ((JmsConnection) connection).getClientID());
                        if (transacted) {
                            LOG.info("Session is transacted");
                            session = connection.createSession(true, Session.SESSION_TRANSACTED);
                        } else {
                            session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
                        }

                        if (selector != null && !selector.isEmpty()) {
                            LOG.info("With selector: " + selector);
                            if (isDurableSub) {
                                consumer = session.createDurableSubscriber((Topic) destination, connection.getClientID(), selector, true);
                            } else {
                                consumer = session.createConsumer(destination, selector);
                            }
                        } else {
                            if (isDurableSub) {
                                consumer = session.createDurableSubscriber((Topic) destination, connection.getClientID());
                            } else {
                                consumer = session.createConsumer(destination);
                            }
                        }
                        if (transacted && transactionIsBatch) {
                            timer.scheduleAtFixedRate(new CommitTask(), transactionDelay, transactionDelay);
                        }
                        connected = true;
                    } catch (JMSException ex) {
                        connected = false;
                        LOG.error("Unhandled exception while connecting.  Retrying...");
                        ex.printStackTrace();
                        if (consumer != null) {
                            try {
                                LOG.info("Closing consumer: {}", this.threadNum);
                                consumer.close();
                            } catch (JMSException exC) {
                                LOG.error("Caught JMSException: ", exC);
                            }
                        }
                        if (session != null) {
                            try {
                                LOG.info("Closing session: {}", this.threadNum);
                                session.close();
                            } catch (JMSException exS) {
                                LOG.error("Caught JMSException: ", exS);
                            }
                        }
                        if (connection != null) {
                            try {
                                LOG.info("Closing connection: {}", this.threadNum);
                                connection.close();
                            } catch (JMSException e) {
                                LOG.error("Error closing connection", e);
                            }
                        }
                    }
                }

                try {

                    if (readDelay > 0) {
                        Thread.sleep(readDelay);
                    }

                    Message message = consumer.receive(messageTimeoutMs);

                    if (message != null) {

                        if (isThrowException) {

                            session.close();
                        }

                        msgsRecd++;
                        LOG.info("Thread {}: {} : Got message {}; with priority {}; total: {}", this.threadNum, System.currentTimeMillis(), message.getJMSMessageID(), message.getJMSPriority(), msgsRecd);

                        if (message.getJMSReplyTo() != null) {
                            replySession = connection.createSession(transacted, Session.AUTO_ACKNOWLEDGE);
                            MessageProducer producer = replySession.createProducer(message.getJMSReplyTo());
                            producer.setTimeToLive(1000);
                            String response = "Received: " + message.getJMSMessageID();
                            LOG.info("Sending Response: " + response);
                            TextMessage responseMsg = (TextMessage) org.apache.qpid.proton.message.Message.Factory.create();
                            responseMsg.setText(response);
                            producer.send(responseMsg);
                            producer.close();
                            replySession.close();
                            replySession = null;
                        }
                        Thread.sleep(transactionDelay);
                        if (transacted && !transactionIsBatch) {
                            try {
                                session.commit();
                                LOG.info("Thread {}: Committed session for message {}.", this.threadNum, message.getJMSMessageID());
                            } catch (TransactionRolledBackException ex) {
                                LOG.info("Thread {}: Rolling back message {}.", this.threadNum, message.getJMSMessageID());
                                session.rollback();
                            }
                        } else {
                            if (isNoAck) {
                                session.recover();
                            } else {
                                message.acknowledge();
                            }
                            LOG.info("Thread {}: Acked message {}.", this.threadNum, message.getJMSMessageID());
                        }
                        if (transacted && isCommit) {
                            try {
                                session.commit();
                                isCommit = false;
                                LOG.info("Thread {}: Acked message {}.", this.threadNum, message.getJMSMessageID());
                            } catch (TransactionRolledBackException ex) {
                                LOG.info("Thread {}: Rolling back message {}.", this.threadNum, message.getJMSMessageID());
                                session.rollback();
                            }
                        }
                    }
                } catch (JMSException eJMS) {
                    LOG.error("Caught JMSException: ", eJMS);
                    if (consumer != null) {
                        try {
                            LOG.info("Closing consumer: {}", this.threadNum);
                            consumer.close();
                        } catch (JMSException exC) {
                            LOG.error("Caught JMSException: ", exC);
                        }
                    }
                    if (session != null) {
                        try {
                            LOG.info("Closing session: {}", this.threadNum);
                            session.close();
                        } catch (JMSException exS) {
                            LOG.error("Caught JMSException: ", exS);
                        }
                    }
                    if (connection != null) {
                        try {
                            LOG.info("Closing connection: {}", this.threadNum);
                            connection.close();
                        } catch (JMSException e) {
                            LOG.error("Error closing connection", e);
                        }
                    }
                    connected = false;
                } catch (InterruptedException eI) {
                    LOG.error("Caught InterruptedException: ", eI);
                    isDone = true;
                }
            }
        } finally {
            if (consumer != null) {
                try {
                    LOG.info("Closing consumer: {}", this.threadNum);
                    consumer.close();
                } catch (JMSException ex) {
                    LOG.error("Caught JMSException: ", ex);
                }
            }
            if (session != null) {
                try {
                    LOG.info("Closing session: {}", this.threadNum);
                    session.close();
                } catch (JMSException ex) {
                    LOG.error("Caught JMSException: ", ex);
                }
            }
            if (connection != null) {
                try {
                    LOG.info("Closing connection: {}", this.threadNum);
                    connection.close();
                } catch (JMSException e) {
                    LOG.error("Error closing connection", e);
                }
            }
        }
    }

    public boolean isDone() {
        return isDone;

    }

    class CommitTask extends TimerTask {

        @Override
        public void run() {
            LOG.info("Dest: " + destination + " : Thread " + threadNum + ": Committing session...");
            isCommit = true;
        }
    }

}
