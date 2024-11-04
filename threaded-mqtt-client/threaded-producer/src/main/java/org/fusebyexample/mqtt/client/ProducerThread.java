/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.fusebyexample.mqtt.client;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.fusesource.mqtt.client.MQTT;
import javax.net.ssl.SSLContext;
import org.fusesource.hawtbuf.AsciiBuffer;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.ByteArrayOutputStream;
import org.fusesource.hawtbuf.UTF8Buffer;
import org.fusesource.hawtdispatch.Task;
import org.fusesource.mqtt.client.Callback;
import org.fusesource.mqtt.client.CallbackConnection;
import org.fusesource.mqtt.client.QoS;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author dhawkins
 */
public class ProducerThread extends Thread {

    private final MQTT mqtt = new MQTT();
    private UTF8Buffer topic;

    private final CallbackConnection connection;
    private final CountDownLatch done = new CountDownLatch(1);

    private final boolean retain;
    private final int messageLength;
    private final int count;
    private final int threadNum;
    private final long delay;

    private final QoS qos;

    private String body = null;

    private static final Logger LOG = LoggerFactory.getLogger(ProducerThread.class);

    public ProducerThread(URI remoteAddress, String user, String password, String destination, String version, QoS qos, short keepAlive, int sendBufferSize, long maxConnectAttempts, long maxReconnectAttempts, long reconnectDelay, long maxReconnectDelay, double backOffMultiplier, int messageLength, int delay, int count, boolean retain, int threadNum) throws NoSuchAlgorithmException {
        if (SSLContext.getDefault() != null) {
            this.mqtt.setSslContext(SSLContext.getDefault());
        }

        this.mqtt.setHost(remoteAddress);
        this.mqtt.setUserName(user);
        this.mqtt.setPassword(password);
        this.mqtt.setVersion(version);
        this.mqtt.setKeepAlive(keepAlive);
        this.mqtt.setConnectAttemptsMax(maxConnectAttempts);
        this.mqtt.setSendBufferSize(sendBufferSize);
        this.mqtt.setReconnectAttemptsMax(maxReconnectAttempts);
        this.mqtt.setReconnectDelay(reconnectDelay);
        this.mqtt.setReconnectDelayMax(maxReconnectDelay);
        this.mqtt.setReconnectBackOffMultiplier(backOffMultiplier);

        this.topic = new UTF8Buffer(destination);
        this.connection = mqtt.callbackConnection();
        this.messageLength = messageLength;
        this.delay = delay;
        this.count = count;
        this.qos = qos;
        this.retain = retain;
        this.threadNum = threadNum;
        
        LOG.info("Starting thread: " + threadNum + " for topic: " + topic.toString() +  " with count: " + count);
    }

    @Override
    public void run() {

        // Handle a Ctrl-C event cleanly.
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                setName("MQTT client shutdown");
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Disconnecting the client.");
                }
                connection.getDispatchQueue().execute(new Task() {
                    @Override
                    public void run() {
                        connection.disconnect(new Callback<Void>() {
                            @Override
                            public void onSuccess(Void value) {
                                done.countDown();
                            }

                            @Override
                            public void onFailure(Throwable value) {
                                done.countDown();
                            }
                        });
                    }
                });
            }
        });

        connection.listener(new org.fusesource.mqtt.client.Listener() {

            @Override
            public void onConnected() {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Connected");
                }
            }

            @Override
            public void onDisconnected() {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Disconnected");
                }
            }

            @Override
            public void onPublish(UTF8Buffer topic, Buffer body, Runnable ack) {
            }

            @Override
            public void onFailure(Throwable value) {

                if (LOG.isDebugEnabled()) {

                    StringWriter error = new StringWriter();
                    value.printStackTrace(new PrintWriter(error));
                    LOG.debug(error.toString());
                } else {
                    LOG.error(value.getLocalizedMessage());
                }
            }
        });

        connection.resume();
        connection.connect(new Callback<Void>() {
            @Override
            public void onFailure(Throwable value) {

                if (LOG.isDebugEnabled()) {

                    StringWriter error = new StringWriter();
                    value.printStackTrace(new PrintWriter(error));
                    LOG.debug(error.toString());
                } else {
                    LOG.error(value.getLocalizedMessage());
                }
            }

            @Override
            public void onSuccess(Void value) {
            }
        });

        new Task() {
            private long sent = 0;

            @Override
            public void run() {
                final Task publish = this;
                Buffer message;
                if (body != null && !body.isEmpty()) {
                    message = new Buffer(body.getBytes());
                } else {
                    body = new RandomString(messageLength).nextString();
                    message = new Buffer(body.getBytes());
                }
                LOG.debug("Sending message with body: " + body.toString());
                try {
                    long id = sent + 1;
                    ByteArrayOutputStream os = new ByteArrayOutputStream(message.length + 15);
                    os.write(new AsciiBuffer(Integer.toString(threadNum)));
                    os.write(':');
                    os.write(new AsciiBuffer(Long.toString(id)));
                    os.write(':');
                    os.write(body.getBytes());
                    message = os.toBuffer();
                    body = null;
                } catch (IOException eio) {
                    LOG.error("Publish failed: " + eio);
                    if (LOG.isDebugEnabled()) {

                        StringWriter error = new StringWriter();
                        eio.printStackTrace(new PrintWriter(error));
                        LOG.debug(error.toString());
                    } else {
                        LOG.error(eio.getLocalizedMessage());
                    }
                }
                connection.publish(topic, message, QoS.AT_MOST_ONCE, retain, new Callback<Void>() {
                    public void onSuccess(Void value) {
                        sent++;
                        if (LOG.isInfoEnabled()) {
                            LOG.info("Sent message #" + sent);
                        }
                        if (sent < count) {
                            if (delay > 0) {
                                LOG.debug("Sleeping");
                                connection.getDispatchQueue().executeAfter(delay, TimeUnit.MILLISECONDS, publish);
                            } else {
                                connection.getDispatchQueue().execute(publish);
                            }
                        } else {
                            connection.disconnect(new Callback<Void>() {
                                public void onSuccess(Void value) {
                                    done.countDown();
                                }

                                public void onFailure(Throwable value) {
                                    done.countDown();
                                }
                            });
                        }
                    }

                    public void onFailure(Throwable value) {
                        LOG.error("Publish failed: " + value);
                        if (LOG.isDebugEnabled()) {

                            StringWriter error = new StringWriter();
                            value.printStackTrace(new PrintWriter(error));
                            LOG.debug(error.toString());
                        } else {
                            LOG.error(value.getLocalizedMessage());
                        }
                    }
                });
            }
        }.run();

        try {
            done.await();
        } catch (InterruptedException ex) {
            if (LOG.isDebugEnabled()) {

                StringWriter error = new StringWriter();
                ex.printStackTrace(new PrintWriter(error));
                LOG.debug(error.toString());
            } else {
                LOG.error(ex.getLocalizedMessage());
            }
        }
    }
}
