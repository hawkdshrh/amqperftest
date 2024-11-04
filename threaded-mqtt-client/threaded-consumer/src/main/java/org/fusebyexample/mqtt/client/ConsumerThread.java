/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.fusebyexample.mqtt.client;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import javax.net.ssl.SSLContext;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.UTF8Buffer;
import org.fusesource.hawtdispatch.Task;
import org.fusesource.mqtt.client.Callback;
import org.fusesource.mqtt.client.CallbackConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author dhawkins
 */
public class ConsumerThread extends Thread {

    private final MQTT mqtt = new MQTT();
    private final ArrayList<org.fusesource.mqtt.client.Topic> topics = new ArrayList<>();

    private final CallbackConnection connection;
    private final CountDownLatch done = new CountDownLatch(1);

    private static final Logger LOG = LoggerFactory.getLogger(ConsumerThread.class);

    public ConsumerThread(URI remoteAddress, String user, String password, String destination, String version, QoS qos, short keepAlive, int maxReadRate, int receiveBufferSize, long maxConnectAttempts, long maxReconnectAttempts, long reconnectDelay, long maxReconnectDelay, double backOffMultiplier, boolean cleanSession, String clientId) throws NoSuchAlgorithmException {
        if (SSLContext.getDefault() != null) {
            this.mqtt.setSslContext(SSLContext.getDefault());
        }

        this.mqtt.setHost(remoteAddress);
        this.mqtt.setUserName(user);
        this.mqtt.setPassword(password);
        this.mqtt.setVersion(version);
        this.mqtt.setKeepAlive(keepAlive);
        this.mqtt.setMaxReadRate(maxReadRate);
        this.mqtt.setCleanSession(cleanSession);
        this.mqtt.setClientId(clientId);
        this.mqtt.setConnectAttemptsMax(maxConnectAttempts);
        this.mqtt.setReceiveBufferSize(receiveBufferSize);
        this.mqtt.setReconnectAttemptsMax(maxReconnectAttempts);
        this.mqtt.setReconnectDelay(reconnectDelay);
        this.mqtt.setReconnectDelayMax(maxReconnectDelay);
        this.mqtt.setReconnectBackOffMultiplier(backOffMultiplier);

        this.topics.add(new Topic(destination, qos));
        this.connection = mqtt.callbackConnection();
    }

    @Override
    public void run() {

        // Handle a Ctrl-C event cleanly.
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                setName("MQTT client shutdown");
                if (LOG.isInfoEnabled()) {
                    LOG.info("Disconnecting the client.");
                }
                connection.getDispatchQueue().execute(new Task() {
                    @Override
                    public void run() {
                        LOG.info("Unsubscribing from " + topics.get(0).name());
                        connection.unsubscribe(new UTF8Buffer[] {topics.get(0).name()}, null);
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
                if (LOG.isInfoEnabled()) {
                    LOG.info("Connected ###");
                }
            }

            @Override
            public void onDisconnected() {
                if (LOG.isInfoEnabled()) {
                    LOG.info("Disconnected ###");
                }
            }

            @Override
            public void onPublish(UTF8Buffer topic, Buffer body, Runnable ack) {

                LOG.info("Received message: ");
                LOG.info("");
                LOG.info("Topic: " + topic);
                LOG.info(body.toString());
                LOG.info("");

                ack.run();
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
                System.exit(2);
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
                System.exit(2);
            }

            @Override
            public void onSuccess(Void value) {
                final Topic[] ta = topics.toArray(new Topic[topics.size()]);
                connection.subscribe(ta, new Callback<byte[]>() {
                    @Override
                    public void onSuccess(byte[] value) {

                        if (LOG.isDebugEnabled()) {
                            for (int i = 0; i < value.length; i++) {
                                LOG.debug("Subscribed to Topic: " + ta[i].name() + " with QoS: " + ta[i].qos());
                            }
                        }
                    }

                    @Override
                    public void onFailure(Throwable value) {

                        LOG.error("Subscribe failed: " + value);
                        if (LOG.isDebugEnabled()) {

                            StringWriter error = new StringWriter();
                            value.printStackTrace(new PrintWriter(error));
                            LOG.debug(error.toString());
                        } else {
                            LOG.error(value.getLocalizedMessage());
                        }
                        System.exit(2);
                    }
                });
            }
        });

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
            System.exit(0);
        }
    }
}
