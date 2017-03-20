package com.gong.kafka;
import java.util.Map;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import storm.kafka.bolt.mapper.TupleToKafkaMapper;
import storm.kafka.bolt.selector.KafkaTopicSelector;
import storm.kafka.bolt.selector.DefaultTopicSelector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class ToKafkaBolt<K,V> extends BaseRichBolt {
	 private static final Logger Log = LoggerFactory.getLogger(ToKafkaBolt.class);
	    
	    public static final String TOPIC = "topic";
	    public static final String KAFKA_BROKER_PROPERTIES = "kafka.broker.properties";

	    private Producer<K, V> producer;
	    private OutputCollector collector;
	    private TupleToKafkaMapper<K, V> Mapper;
	    private KafkaTopicSelector topicselector;
	    
	    public ToKafkaBolt<K,V> withTupleToKafkaMapper(TupleToKafkaMapper<K, V> mapper){
	        this.Mapper = mapper;
	        return this;
	    }
	    
	    public ToKafkaBolt<K, V> withTopicSelector(KafkaTopicSelector topicSelector){
	        this.topicselector = topicSelector;
	        return this;
	    }
	    
	    public void prepare(Map stormConf, TopologyContext context,
	            OutputCollector collector) {
	        
	        if (Mapper == null) {
	            this.Mapper = new FieldNameBasedTupleToKafkaMapper<K, V>();
	        }
	        
	        if (topicselector == null) {
	            this.topicselector = new DefaultTopicSelector((String)stormConf.get(TOPIC));
	        }
	        
	        Map configMap = (Map) stormConf.get(KAFKA_BROKER_PROPERTIES);
	        Properties properties = new Properties();
	        properties.putAll(configMap);
	        ProducerConfig config = new ProducerConfig(properties);
	        producer = new Producer<K, V>(config);
	        this.collector = collector;
	    }

	    
	    public void execute(Tuple input) {
//	        String iString = input.getString(0);
	        
	        K key = null;
	        V message = null;
	        String topic = null;
	        
	        try {
	            
	            key = Mapper.getKeyFromTuple(input);
	            message = Mapper.getMessageFromTuple(input);
	            topic = topicselector.getTopic(input);
	            if (topic != null) {
	                producer.send(new KeyedMessage<K, V>(topic,message));
	                
	            }else {
	                Log.warn("skipping key = "+key+ ",topic selector returned null.");
	            }
	    
	        } catch ( Exception e) {
	            // TODO: handle exception
	            Log.error("Could not send message with key = " + key
	                    + " and value = " + message + " to topic = " + topic, e);
	        }finally{
	            collector.ack(input);
	        }
	    }

	    
	    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	    }
	    

}
