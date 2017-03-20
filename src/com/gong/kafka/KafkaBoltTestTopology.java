package com.gong.kafka;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import storm.kafka.trident.TridentKafkaState;

import java.util.Arrays;
import java.util.Properties;

import com.esotericsoftware.minlog.Log;


public class KafkaBoltTestTopology {
	 //配置kafka spout参数
    public static String kafka_zk_port = null;
    public static String topic = null;
    public static String kafka_zk_rootpath = null;
    public static BrokerHosts brokerHosts;
    public static String spout_name = "spout";
    public static String kafka_consume_from_start = null;
    
    public static class PrinterBolt extends BaseBasicBolt {

        /**
         * 
         */
            private static final long serialVersionUID = 9114512339402566580L;

            //    @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {
            }

         //   @Override
            public void execute(Tuple tuple, BasicOutputCollector collector) {
                System.out.println("-----"+(tuple.getValue(1)).toString());
            }

        }
        
    public StormTopology buildTopology(){
        //kafkaspout 配置文件
        kafka_consume_from_start = "true";
        kafka_zk_rootpath = "/master";
        String spout_id = spout_name;
        brokerHosts = new ZkHosts("172.20.104.92:2191", kafka_zk_rootpath+"/brokers");
        kafka_zk_port = "2191";
        SpoutConfig spoutConf = new SpoutConfig(brokerHosts, "testfromkafka", kafka_zk_rootpath, spout_id);
        spoutConf.scheme = new SchemeAsMultiScheme(new MessageScheme());
        spoutConf.zkPort = Integer.parseInt(kafka_zk_port);
        spoutConf.zkRoot = kafka_zk_rootpath;
        spoutConf.zkServers = Arrays.asList(new String[] {"172.20.104.92"});
        
        //是否從kafka第一條數據開始讀取
        if (kafka_consume_from_start == null) {
            kafka_consume_from_start = "false";
        }
        boolean kafka_consume_frome_start_b = Boolean.valueOf(kafka_consume_from_start);
        if (kafka_consume_frome_start_b != true && kafka_consume_frome_start_b != false) {
            System.out.println("kafka_comsume_from_start must be true or false!");
        }
        System.out.println("kafka_consume_from_start: " + kafka_consume_frome_start_b);
        spoutConf.forceFromStart=kafka_consume_frome_start_b;
        
        
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new KafkaSpout(spoutConf));
        builder.setBolt("forwardToKafka", new ToKafkaBolt<String, String>()).shuffleGrouping("spout");
        return builder.createTopology();
    }

    public static void main(String[] args) {
        
        KafkaBoltTestTopology kafkaBoltTestTopology = new KafkaBoltTestTopology();
        StormTopology stormTopology = kafkaBoltTestTopology.buildTopology();

        Config conf = new Config();
        //设置kafka producer的配置
        Properties props = new Properties();
        props.put("metadata.broker.list", "172.20.104.92:9092");
        props.put("producer.type","async");
        props.put("request.required.acks", "0"); // 0 ,-1 ,1
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        conf.put(TridentKafkaState.KAFKA_BROKER_PROPERTIES, props);
        conf.put("topic","test");
        Log.info("the connect information ---------------------------");
        Log.info("the connect information ---------------------------");

        Log.info("the connect information ---------------------------");

        if(args.length > 0){
            // cluster submit.
            try {
                 StormSubmitter.submitTopology("kafkaboltTest", conf, stormTopology);
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            }
        }else{
            new LocalCluster().submitTopology("kafkaboltTest", conf, stormTopology);
        }
        Log.info("the connect information ---------------------------");
        Log.info("the connect information ---------------------------");
        Log.info("the connect information ---------------------------");

    }
}
