package org.fusebyexample.openwire_client.simple;

import java.util.Map;
import java.util.Random;
import java.util.logging.Level;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
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
    private final int generateStringHeaderSize;
    private final long ttl;
    private final boolean messageLengthFixed;
    private final boolean transacted;
    private final boolean persistent;
    private final boolean dynamic;
    private final boolean replyTo;
    private final boolean useMessageGrouping;
    private final boolean generateStringHeader;
    private boolean isDone = false;
    private final String messageBody;
    private final String generateStringHeaderName;
    private final Destination replyToAddress;
    private final Map<String, String> additionalStringHeaders;
    private final Map<String, Integer> additionalIntHeaders;
    private final Map<String, Long> additionalLongHeaders;
    private final Map<String, Double> additionalDblHeaders;
    private final Map<String, Boolean> additionalBoolHeaders;
    private static final Logger LOG = LoggerFactory.getLogger(ProducerThread.class);

    public ProducerThread(Connection connection, ConnectionFactory factory, Destination destination, String clientPrefix, int iterations, int delay, long ttl, int threadNum, int messageLength, boolean messageLengthFixed, boolean transacted, boolean persistent, boolean dynamic, String messageBody, Map stringHeaders, Map intHeaders, Map longHeaders, Map dblHeaders, Map boolHeaders, boolean replyTo, Destination replyToAddress, boolean useMessageGrouping, boolean generateStringHeader, String generateStringHeaderName, int generateStringHeaderSize) {
        this.connection = connection;
        this.factory = factory;
        this.destination = destination;
        this.clientPrefix = clientPrefix;
        this.iterations = iterations;
        this.delay = delay;
        this.ttl = ttl;
        this.threadNum = threadNum;
        this.messageLength = messageLength;
        this.messageLengthFixed = messageLengthFixed;
        this.transacted = transacted;
        this.persistent = persistent;
        this.dynamic = dynamic;
        this.messageBody = messageBody;
        this.additionalStringHeaders = stringHeaders;
        this.additionalIntHeaders = intHeaders;
        this.additionalLongHeaders = longHeaders;
        this.additionalDblHeaders = dblHeaders;
        this.additionalBoolHeaders = boolHeaders;
        this.replyTo = replyTo;
        this.replyToAddress = replyToAddress;
        this.useMessageGrouping = useMessageGrouping;
        this.generateStringHeader = generateStringHeader;
        this.generateStringHeaderName = generateStringHeaderName;
        this.generateStringHeaderSize = generateStringHeaderSize;
    }

    @Override
    public void run() {

        while (!isDone) {

            try {
                MessageConsumer responseConsumer = null;
                Destination tempDest = null;

                if (connection == null) {
                    connection = factory.createConnection();
                    connection.setClientID(clientPrefix + "." + destination + "-" + Integer.toString(threadNum));
                    connection.start();
                }
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
                    if (replyToAddress != null) {
                        responseConsumer = session.createConsumer(replyToAddress);
                        responseConsumer.setMessageListener(this);
                    } else {
                        tempDest = session.createTemporaryQueue();
                        responseConsumer = session.createConsumer(tempDest);
                        responseConsumer.setMessageListener(this);
                    }
                }

                String padding = null;
                Random random = new Random();

                if (messageBody == null && messageLengthFixed) {
                    LOG.info("Building random message body");
                    StringBuilder builder = new StringBuilder();
                    for (int j = 0; j < messageLength; j++) {
                        builder.append((char) (random.nextInt(94) + 32));
                    }
                    padding = builder.toString();
                } else {
                    LOG.info("Using message body: " + messageBody);
                }

                LOG.info("Starting Production at: {} for {}", System.currentTimeMillis(), connection.getClientID());
                for (int i = 1; i <= iterations; i++) {

                    try {
                        TextMessage message = null;
                        if (messageBody != null) {
                            message = session.createTextMessage(messageBody);
                        } else {
                            //message = session.createTextMessage(Integer.toString(i));
                            message = session.createTextMessage("Sending message " + i + " at " + System.currentTimeMillis() + ": " + padding + ": to " + producer.getDestination() + " from " + threadNum);
                        }

                        if (generateStringHeader && generateStringHeaderName != null && generateStringHeaderName != "" && generateStringHeaderSize > 0) {
                            LOG.info("Building random header of length: " + generateStringHeaderSize);
                            StringBuilder headerBuilder = new StringBuilder();
                            for (int j = 0; j < generateStringHeaderSize; j++) {
                                headerBuilder.append((char) (random.nextInt(94) + 32));
                            }
                            String header = headerBuilder.toString();
                            message.setStringProperty(generateStringHeaderName, header);
                        }

                        if (replyTo) {
                            message.setJMSReplyTo(tempDest);
                        }

                        if (useMessageGrouping) {
                            message.setStringProperty("JMSXGroupID", Integer.toString(this.threadNum));
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
                        if (transacted) {
                            session.commit();
                        }
                        LOG.info("{} : Thread {}: Sent {}. message: {}", System.currentTimeMillis(), threadNum, i, message.getJMSMessageID());

                    } catch (JMSException ex) {
                        LOG.error("Caught JMSException: ", ex);
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

                LOG.info("Closing session objects...");

                if (producer != null) {
                    LOG.info("Closing producer...");
                    try {
                        producer.close();
                    } catch (JMSException ex) {
                        LOG.error("Caught JMSException: ", ex);
                    }
                }
                if (session != null) {
                    LOG.info("Closing session...");
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
    public void onMessage(Message message
    ) {
        String messageText = null;
        try {
            if (message instanceof TextMessage) {
                TextMessage textMessage = (TextMessage) message;
                messageText = textMessage.getText();
                LOG.info("messageText = " + messageText);
            }
            if (message instanceof MapMessage) {
                MapMessage mapMessage = (MapMessage) message;
                if (mapMessage.getString("destinationName") != null) {
                    LOG.info("destinationName = " + mapMessage.getString("destinationName"));
                }
                if (mapMessage.getLong("lastTimestamp") != 0) {
                    LOG.info("lastTimestamp = " + mapMessage.getLong("lastTimestamp"));
                }
                if (mapMessage.getLong("firstTimestamp") != 0) {
                    LOG.info("firstTimestamp = " + mapMessage.getLong("firstTimestamp"));
                }
            }
        } catch (JMSException e) {
            //Handle the exception appropriately
        }
    }

    public boolean isDone() {
        return isDone;
    }

}
