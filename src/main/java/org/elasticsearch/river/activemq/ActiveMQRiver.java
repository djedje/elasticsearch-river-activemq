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
import java.util.concurrent.ThreadFactory;
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
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
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
    
    public final String defaultActiveMQUser = ActiveMQConnectionFactory.DEFAULT_USER;
    public final String defaultActiveMQPassword = ActiveMQConnectionFactory.DEFAULT_PASSWORD;
    public final String defaultActiveMQBrokerUrl = ActiveMQConnectionFactory.DEFAULT_BROKER_URL;
    public static final String defaultActiveMQSourceType = "queue"; // topic
    public static final String defaultActiveMQSourceName = "elasticsearch";
    
    public static final String DEFAULT_TYPE = "messageLost";
    public static final String DEFAULT_INDEX = "lost";
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
    
    private volatile Thread thread;
    
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
            activeMQSourceType = XContentMapValues.nodeStringValue(activeMQSettings.get("sourceType"), defaultActiveMQSourceType);
            activeMQSourceType = activeMQSourceType.toLowerCase();
            if (!"queue".equals(activeMQSourceType) && !"topic".equals(activeMQSourceType))
                throw new IllegalArgumentException("Specified an invalid source type for the ActiveMQ River. Please specify either 'queue' or 'topic'");
            activeMQSourceName = XContentMapValues.nodeStringValue(activeMQSettings.get("sourceName"), defaultActiveMQSourceName);
            activeMQConsumerName = XContentMapValues.nodeStringValue(activeMQSettings.get("consumerName"), defaultActiveMQConsumerName);
            activeMQCreateDurableConsumer = XContentMapValues.nodeBooleanValue(activeMQSettings.get("durable"), defaultActiveMQCreateDurableConsumer);
            activeMQTopicFilterExpression = XContentMapValues.nodeStringValue(activeMQSettings.get("filter"), defaultActiveMQTopicFilterExpression);
            
        } else {
            activeMQUser = (defaultActiveMQUser);
            activeMQPassword = (defaultActiveMQPassword);
            activeMQBrokerUrl = (defaultActiveMQBrokerUrl);
            activeMQSourceType = (defaultActiveMQSourceType);
            activeMQSourceName = (defaultActiveMQSourceName);
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
        ThreadFactory daemonThreadFactory = EsExecutors.daemonThreadFactory(settings.globalSettings(), "activemq_river");
        //TODO voir si c'est judicieux le ThreadExecutor
        //TODO se faire injecter un ThreadPool géré par ES directement à l'instanciation de la River comme la River Twitter.
        executor = Executors.newSingleThreadExecutor(daemonThreadFactory);
        executor.submit(new Consumer(connectionFactory, activeMQConsumerName));
    }
    
    
    @Override
    public void close() {
        if (!isRiverOpened) {
            return;
        }
        logger.info("Closing ActiveMQ river");
        isRiverOpened = false;
        //TODO flush remaining message before shutdown
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
            while (isRiverOpened) {
                
                try {
                    connection = connectionFactory.createConnection();
                    //TODO if no operation but error on connection set ExceptionListener
                    // connection.setExceptionListener(null);
                    
                    connection.setClientID(consumerName);
                    connection.start();
                    
                    final boolean isTransacted = false;
                    Session session = connection.createSession(isTransacted, Session.AUTO_ACKNOWLEDGE);
                    
                    Destination destination = createDestination(session);
                    
                    MessageConsumer consumer = createConsumer(session, destination);
                    //TODO If auto acknowledge create BulkProcessor Listener
                    BulkProcessor bulkProcessor = createBulkProcessor();
                    while(isRiverOpened) {
                        Message message = consumer.receive(); // synchronous receive // TODO asynchronous message with MessageListener ?
                        if (!(message instanceof TextMessage)) {
                            //TODO message type not supported, skip it
                            continue;
                        }
                        
                        TextMessage txtMessage = (TextMessage) message;
                        sendIntoES(txtMessage, bulkProcessor);
                    }
                    bulkProcessor.close(); // flushing
                    
                } catch (JMSException e) {
                    cleanup(connection, "failed to connect");
                    //reconnect
                }
            }
            cleanup(connection, "closing river");
            
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
            BulkProcessor bulkProcessor = BulkProcessor.builder(client,
                    new BulkProcessor.Listener() {
                        
                        @Override
                        public void beforeBulk(long executionId,
                                BulkRequest request) {
                            logger.info("Going to execute new bulk composed of {} actions",
                                    request.numberOfActions());
                        }
                        
                        @Override
                        public void afterBulk(long executionId,
                                BulkRequest request,
                                BulkResponse response) {
                            logger.info("Executed bulk composed of {} actions",
                                    request.numberOfActions());
                            for (Object payload : request.payloads()) {
                                if (payload == null) {
                                    continue;
                                }
                                Message jmsMessage = (Message) payload;
                                try {
                                    logger.info("Jms Message send {}", jmsMessage.getJMSMessageID());
                                    jmsMessage.acknowledge(); // TODO !! ahah
                                } catch (JMSException ex) {
                                }
                            }
                        }
                        
                        @Override
                        public void afterBulk(long executionId,
                                BulkRequest request,
                                Throwable failure) {
                            logger.warn("Error executing bulk", failure);
                        }
                    }).setBulkActions(bulkSize).setFlushInterval(bulkTimeout).build();
            //TODO Take care of ordered field  value to set concurrent bulkprocessors
            
            return bulkProcessor;
        }
        
        private void sendIntoES(TextMessage message, BulkProcessor bulkProcessor) {
            
            try {
                String content = message.getText();
                bulkProcessor.add(new BytesArray(content), false, DEFAULT_INDEX, DEFAULT_TYPE);
            } catch (JMSException e) {
                //TODO Unable to extract content from message
                return;
            } catch (Exception ex) {
                //TODO Unable to parse Bulk action or message; skip it.
            }
        }
        
        
        private void cleanup(Connection connection, String message) {
            try {
                connection.close();
            } catch (JMSException e) {
                logger.debug("failed to close connection on [{}]", e, message);
            }
        }
    }
    
}
