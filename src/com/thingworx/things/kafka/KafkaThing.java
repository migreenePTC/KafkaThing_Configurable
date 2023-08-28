package com.thingworx.things.kafka;

import ch.qos.logback.classic.Logger;
import com.thingworx.data.util.InfoTableInstanceFactory;
import com.thingworx.datashape.DataShape;
import com.thingworx.entities.utils.EntityUtilities;
import com.thingworx.logging.LogUtilities;
import com.thingworx.metadata.annotations.*;
import com.thingworx.relationships.RelationshipTypes;
import com.thingworx.things.Thing;
import com.thingworx.types.BaseTypes;
import com.thingworx.types.InfoTable;
import com.thingworx.types.collections.ValueCollection;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.*;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("serial")
@ThingworxConfigurationTableDefinitions(
        tables = {@ThingworxConfigurationTableDefinition(
                name = "ConnectionInfo",
                description = "Connection Settings",
                isMultiRow = false,
                dataShape = @ThingworxDataShapeDefinition(
                        fields = {@ThingworxFieldDefinition(
                                name = "serverName",
                                description = "KafkaServer:port",
                                baseType = "STRING",
                                aspects = {"defaultValue:broker:29092"}
                        ), @ThingworxFieldDefinition(
                                name = "clientID",
                                description = "Client Name",
                                baseType = "STRING",
                                aspects = {"defaultValue:TestConsumer1"}
                        ), @ThingworxFieldDefinition(
                                name = "topicName",
                                description = "Topic Name",
                                baseType = "STRING",
                                aspects = {"defaultValue:Thingworx"}
                        ), @ThingworxFieldDefinition(
                                name = "groupID",
                                description = "Consumer Group Name",
                                baseType = "STRING",
                                aspects = {"defaultValue:TWXTestGroup"}
                        ), @ThingworxFieldDefinition(
                                name = "timeout",
                                description = "Max No Message Found",
                                baseType = "NUMBER",
                                aspects = {"defaultValue:100"}
                                
                        )}
                )
        )}
)

public class KafkaThing
        extends Thing {
    protected static final Logger _Logger = LogUtilities.getInstance().getApplicationLogger(KafkaThing.class);

    //TWX Configuration
    private String _serverName = "http://host.docker.internal:9092";
    private String _clientID = "ERMTWX";
    private String _topicName = "Thingworx";
    private String _groupID = "ERMGroup";
    private Integer _timeout = 100;
    

    //KAFKA
    private String KAFKA_BROKERS = this._serverName;
    private String GROUP_ID_CONFIG;

    protected void initializeThing()
            throws Exception {
        this._serverName = (String) this.getConfigurationSetting("ConnectionInfo", "serverName");
        this._clientID = ((String) this.getConfigurationSetting("ConnectionInfo", "clientID"));
        this._topicName = (String) this.getConfigurationSetting("ConnectionInfo", "topicName");
        this._groupID = (String) this.getConfigurationSetting("ConnectionInfo", "groupID");
        this._timeout = ((Number) this.getConfigurationSetting("ConnectionInfo", "timeout")).intValue();

        KAFKA_BROKERS = this._serverName;
        GROUP_ID_CONFIG = this._groupID;
    }

    public KafkaThing() {
        _Logger.info("Started the Kafka Extension for Thingworx");
    }

    @ThingworxServiceDefinition(name = "receiveMessages", description = "")
    @ThingworxServiceResult(name = "result", description = "", baseType = "INFOTABLE")
    public InfoTable receiveMessages(@ThingworxServiceParameter(name = "Topic name", description = "at least one field must be defined as string", baseType = "STRING") String topic,
                                          @ThingworxServiceParameter(name = "Message Table Datashape", description = "Data shape for the returned results", baseType = "DATASHAPENAME") String dataShape,
                                          @ThingworxServiceParameter(name = "Maximum Messages Count", description = "Maximum messages to return in the InfoTable", baseType = "NUMBER") Double maxItems)
            {
        if (maxItems == null) {
            maxItems = new Double(50.0D);
        }       
        
        Consumer<Long, String> consumer = null;
        InfoTable it = null;

        	try {
        		
        		DataShape ds = (DataShape) EntityUtilities.findEntity(dataShape, RelationshipTypes.ThingworxRelationshipTypes.DataShape);
                
                consumer = createConsumer(topic);
            //InfoTable it = InfoTableInstanceFactory.createInfoTableFromDataShape(ds.getDataShape());
        		it = InfoTableInstanceFactory.createInfoTableFromDataShape(ds.getDataShape());

            _Logger.info("Start receiving");
            //final Consumer<Long, String> consumer = createConsumer(topic);

            //try {
                long startTime = System.currentTimeMillis();
                while (false||(System.currentTimeMillis()-startTime)<10000) {
                    final ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);
                    for(ConsumerRecord<Long, String> cr : consumerRecords) {
                        _Logger.info("Consumer Record:" + cr.value());

                        ValueCollection values = new ValueCollection();
                        try {
                        	values.put("value", BaseTypes.ConvertToPrimitive(cr.value(), BaseTypes.STRING ));
                            values.put("key", BaseTypes.ConvertToPrimitive(cr.key(), BaseTypes.STRING ));
                            values.put("offset", BaseTypes.ConvertToPrimitive(cr.offset(), BaseTypes.STRING ));
                            values.put("headers", BaseTypes.ConvertToPrimitive(cr.headers(), BaseTypes.STRING ));

                        } catch (Exception e) {
                            _Logger.info("Could not insert value into infotable");
                        }
                        it.addRow(values);
                    }
                    consumer.commitAsync();
                }
            } catch (CommitFailedException e) {
                System.out.println("CommitFailedException: " + e);
            } 
            catch (Exception e) {
            	_Logger.error(e.getMessage());
            }
            finally {
                consumer.close();
            }
            _Logger.info("Finished receiving");
            return it;
    }
    
    @ThingworxServiceDefinition(name = "sendMessage", description = "")
    @ThingworxServiceResult(name = "result", description = "", baseType = "STRING")
    public String sendMessage(@ThingworxServiceParameter(name = "Topic name", description = "at least one field must be defined as string", baseType = "STRING") String topic,
                                      @ThingworxServiceParameter(name = "Message", description = "Content to be published under a topic", baseType = "STRING") String message)
            throws Exception {

        _Logger.info("Start sending message");
        initializeThing();
		initContainerArgs();

        final Producer<String, String> producer = createProducer();

        String key = String.valueOf(ThreadLocalRandom.current().nextInt(1, 1000 + 1));
        final ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, message);
        producer.send(record);

        _Logger.info("Finished sending message ");
        return "Sent Messages";
    }
    
    private Producer<String, String> createProducer() {
        try{
            Properties properties = new Properties();
            properties.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaThingWorxProducer");
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            
            properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
            

            return new KafkaProducer<>(properties);
        } catch (Exception e){
            _Logger.info("Failed to create producer with exception: " + e);
            return null;
        }
    }

    private Consumer<Long, String> createConsumer(String topic) {
        try {
    		initializeThing();
    		initContainerArgs();
    		
    		//System.setProperty("javax.net.debug", "ssl:handshake");
        
            Properties properties = new Properties();

            properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "KafkaThingWorxConsumer#" );
            properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            
            properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
            properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID_CONFIG);
            properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
            
            // Create the consumer using properties
            final Consumer<Long, String> consumer = new KafkaConsumer<>(properties);

            // Subscribe to the topic
            consumer.subscribe(Collections.singletonList(topic));
            return consumer;

        } catch (Exception e){
        	_Logger.error("Exception creating Kafka Consumer: " + e);
            System.exit(1);
            return null;        //unreachable
        }
    }

    private void initContainerArgs () {
        GROUP_ID_CONFIG = this._groupID;
        KAFKA_BROKERS = this._serverName;
    }

    protected static class ConfigConstants {
        public static final String ServerName = "serverName";
        public static final String ClientID = "clientID";
        public static final String TopicName = "topicName";
        public static final String GroupID = "groupID";
        public static final String Timeout = "timeout";

        protected ConfigConstants() {
        }
    }

}

