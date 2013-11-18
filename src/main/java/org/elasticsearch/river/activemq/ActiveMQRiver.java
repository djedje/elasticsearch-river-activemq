/*
* Licensed to ElasticSearch under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. ElasticSearch licenses this
* file to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package org.elasticsearch.river.activemq;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkProcessor.Builder;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

/**
 * &quot;Native&quot; ActiveMQ River for ElasticSearch.
 *
 * @author Dominik Dorn // http://dominikdorn.com
 */
public class ActiveMQRiver extends AbstractRiverComponent implements River {
    
    public static final String DEFAULT_ACTIVEMQ_SOURCE_TYPE = "queue"; // topic
    public static final String DEFAULT_ACTIVEMQ_SOURCE_NAME = "elasticsearch";
    public static final String DEFAULT_TYPE = "unknown_type";
    public static final String DEFAULT_INDEX = "unknown_index";
    public static final int DEFAULT_CONCURRENT_REQUESTS = 5;
    
    public static final String defaultActiveMQUser = ActiveMQConnectionFactory.DEFAULT_USER;
    public static final String defaultActiveMQPassword = ActiveMQConnectionFactory.DEFAULT_PASSWORD;
    public static final String defaultActiveMQBrokerUrl = ActiveMQConnectionFactory.DEFAULT_BROKER_URL;
    public final String defaultActiveMQConsumerName;
    public final boolean defaultActiveMQCreateDurableConsumer = false;
    public final String defaultActiveMQTopicFilterExpression = "";
    
    private final String activeMQUser;
    private final String activeMQPassword;
    private final String activeMQBrokerUrl;
    
    private String activeMQSourceType;
    private String activeMQSourceName;
    private String activeMQConsumerName;
    private boolean activeMQCreateDurableConsumer;
    private String activeMQTopicFilterExpression;
    
    private final Client client;
    
    private final int bulkSize;
    private final TimeValue bulkTimeout;
    private final boolean ordered;
    private volatile boolean isRiverOpened = false;
    
    private ExecutorService executor;
    
    @SuppressWarnings({"unchecked"})
    @Inject
    public ActiveMQRiver(RiverName riverName, RiverSettings settings, Client client) {
        super(riverName, settings);
        this.client = client;
        this.defaultActiveMQConsumerName = "activemq_elasticsearch_river_" + riverName().name();
        
        if (settings.settings().containsKey("activemq")) {
            Map<String, Object> activeMQSettings = (Map<String, Object>) settings.settings().get("activemq");
            activeMQUser = XContentMapValues.nodeStringValue(activeMQSettings.get("user"), defaultActiveMQUser);
            activeMQPassword = XContentMapValues.nodeStringValue(activeMQSettings.get("pass"), defaultActiveMQPassword);
            activeMQBrokerUrl = XContentMapValues.nodeStringValue(activeMQSettings.get("brokerUrl"), defaultActiveMQBrokerUrl);
            activeMQSourceType = XContentMapValues.nodeStringValue(activeMQSettings.get("sourceType"), DEFAULT_ACTIVEMQ_SOURCE_TYPE);
            activeMQSourceType = activeMQSourceType.toLowerCase();
            if (!"queue".equals(activeMQSourceType) && !"topic".equals(activeMQSourceType))
                throw new IllegalArgumentException("Specified an invalid source type for the ActiveMQ River. Please specify either 'queue' or 'topic'");
            activeMQSourceName = XContentMapValues.nodeStringValue(activeMQSettings.get("sourceName"), DEFAULT_ACTIVEMQ_SOURCE_NAME);
            activeMQConsumerName = XContentMapValues.nodeStringValue(activeMQSettings.get("consumerName"), defaultActiveMQConsumerName);
            activeMQCreateDurableConsumer = XContentMapValues.nodeBooleanValue(activeMQSettings.get("durable"), defaultActiveMQCreateDurableConsumer);
            activeMQTopicFilterExpression = XContentMapValues.nodeStringValue(activeMQSettings.get("filter"), defaultActiveMQTopicFilterExpression);
            
        } else {
            activeMQUser = (defaultActiveMQUser);
            activeMQPassword = (defaultActiveMQPassword);
            activeMQBrokerUrl = (defaultActiveMQBrokerUrl);
            activeMQSourceType = (DEFAULT_ACTIVEMQ_SOURCE_TYPE);
            activeMQSourceName = (DEFAULT_ACTIVEMQ_SOURCE_NAME);
            activeMQConsumerName = defaultActiveMQConsumerName;
            activeMQCreateDurableConsumer = defaultActiveMQCreateDurableConsumer;
            activeMQTopicFilterExpression = defaultActiveMQTopicFilterExpression;
        }
        
        if (settings.settings().containsKey("index")) {
            Map<String, Object> indexSettings = (Map<String, Object>) settings.settings().get("index");
            bulkSize = XContentMapValues.nodeIntegerValue(indexSettings.get("bulk_size"), 100);
            if (indexSettings.containsKey("bulk_timeout")) {
                bulkTimeout = TimeValue.parseTimeValue(XContentMapValues.nodeStringValue(indexSettings.get("bulk_timeout"), "10ms"), TimeValue.timeValueMillis(10));
            } else {
                bulkTimeout = TimeValue.timeValueMillis(10);
            }
            ordered = XContentMapValues.nodeBooleanValue(indexSettings.get("ordered"), false);
        } else {
            bulkSize = 100;
            bulkTimeout = TimeValue.timeValueMillis(10);
            ordered = false;
        }
    }
    
    
    @Override
    public void start() {
        logger.info("Creating an ActiveMQ river: user [{}], broker [{}], sourceType [{}], sourceName [{}]",
                activeMQUser,
                activeMQBrokerUrl,
                activeMQSourceType,
                activeMQSourceName
        );
        isRiverOpened = true;
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(activeMQUser, activeMQPassword, activeMQBrokerUrl);
        executor = Executors.newSingleThreadExecutor();
        executor.submit(new Consumer(connectionFactory, activeMQConsumerName));
    }
    
    
    @Override
    public void close() {
        if (!isRiverOpened) {
            return;
        }
        logger.info("Closing ActiveMQ river");
        isRiverOpened = false;
        executor.shutdown();
        
    }
    
    private class Consumer implements Runnable {
        
        private Connection connection;
        private final ConnectionFactory connectionFactory;
        private final String consumerName;
        
        private Consumer(ConnectionFactory connectionFactory, String consumerName) {
            this.connectionFactory = connectionFactory;
            this.consumerName = consumerName;
        }
        
        @Override
        public void run() {
            while (isRiverOpened && !Thread.currentThread().isInterrupted()) {
                
                BulkProcessor bulkProcessor = null;
                try {
                    connection = connectionFactory.createConnection();
                    connection.setClientID(consumerName);
                    connection.start();
                    
                    final boolean isTransacted = false;
                    Session session = connection.createSession(isTransacted, Session.CLIENT_ACKNOWLEDGE); 
                    
                    Destination destination = createDestination(session);
                    
                    MessageConsumer consumer = createConsumer(session, destination);
                    bulkProcessor = createBulkProcessor();
                    while(isRiverOpened) {
                        Message message = consumer.receive(); 
                        if (!(message instanceof TextMessage)) {
                            logger.info("Message type [{}] with ID [{}] not supported, skipped", message.getJMSMessageID(), message.getJMSType());
                            continue;
                        }
                        
                        TextMessage txtMessage = (TextMessage) message;
                        sendIntoES(txtMessage, bulkProcessor);
                    }
                    
                    
                } catch (JMSException e) {
                    logger.error("Error during JMS communication, reconnecting...", e.getMessage());
                } finally {
                    // flushing
                    if (bulkProcessor != null) { bulkProcessor.close(); }
                    cleanup();
                }
            }
            logger.info("CLosing river...done");
        }
        
        private Destination createDestination(Session session) throws JMSException {
            return (activeMQSourceType.equals("queue")) ?
                    session.createQueue(activeMQSourceName) :
                    session.createTopic(activeMQSourceName);
        }
        
        private MessageConsumer createConsumer(Session session, Destination destination) throws JMSException {
            MessageConsumer consumer;
            if (activeMQCreateDurableConsumer && activeMQSourceType.equals("topic")) {
                if (!"".equals(activeMQTopicFilterExpression)) {
                    consumer = session.createDurableSubscriber(
                            (Topic) destination, // topic name
                            activeMQConsumerName, // consumer name
                            activeMQTopicFilterExpression, // filter expression
                            true // ?? TODO - lookup java doc as soon as network connection is back.
                    );
                } else {
                    consumer = session.createDurableSubscriber((Topic) destination, activeMQConsumerName);
                }
            } else {
                consumer = session.createConsumer(destination);
            }
            return consumer;
        }
        
        private BulkProcessor createBulkProcessor() {
            Builder builder = BulkProcessor.builder(client,
                    new BulkProcessor.Listener() {
                        
                        @Override
                        public void beforeBulk(long executionId, BulkRequest request) {
                            logger.info("Going to execute new bulk composed of {} actions", request.numberOfActions());
                        }
                        
                        @Override
                        public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                            logger.info("Executed bulk composed of {} actions", request.numberOfActions());
                            acknowledgeMessages(request);
                        }
                        
                        @Override
                        public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                            logger.warn("Error executing bulk", failure);
                        }
                        
                        private void acknowledgeMessages(BulkRequest request) {
                            for (Object payload : request.payloads()) {
                                if (payload == null) {
                                    continue;
                                }
                                Message jmsMessage = (Message) payload;
                                try {
                                    logger.info("JMS message with ID [{}] sent into ES", jmsMessage.getJMSMessageID());
                                    jmsMessage.acknowledge();
                                } catch (JMSException ex) {
                                    logger.warn("Unable to perform reporting on JMS message after a successful BulkRequest", ex);
                                }
                            }
                        }

                    }).setBulkActions(bulkSize).setFlushInterval(bulkTimeout);
            
            if (!ordered) {
                builder.setConcurrentRequests(DEFAULT_CONCURRENT_REQUESTS);
            }
            
            return builder.build();
        }
        
        private void sendIntoES(TextMessage message, BulkProcessor bulkProcessor) {
            String messageId = null;
            try {
                String content = message.getText();
                messageId = message.getJMSMessageID();
                bulkProcessor.add(new BytesArray(content), false, DEFAULT_INDEX, DEFAULT_TYPE, message);
            } catch (JMSException e) {
                logger.error("Unable to extract message content, message skipped");
            } catch (Exception ex) {
                logger.error("Unable to prepare ES request to send message with ID [{}], message skipped", messageId);
            }
        }
        
        private void cleanup() {
            
            try {
                connection.close();
            } catch (JMSException e) {
                logger.debug("Error during JMS failed to close JMS connection");
            }
        }
    }
    
}
