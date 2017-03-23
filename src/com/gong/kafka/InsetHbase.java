package com.gong.kafka;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.sibat.conf.ConfigureManager;
import com.sibat.constant.Constants;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class InsetHbase {
	private String topic;//the name of the topic
	public InsetHbase(String topic) {
		super();
		this.topic = topic;
	}
	public static Configuration conf;

	static {//configure hbase
		conf = HBaseConfiguration.create();
		conf.set(Constants.HZPCLIENTPROT,"2181");
		conf.set(Constants.HZQUORUM,"172.20.104.92");
		conf.set(Constants.HMASTER, "172.20.104.92:60010");

		System.out.println("配置成功！");
	}
	private ConsumerConnector createConsumer() {//configure kafka
		Properties properties = new Properties();
		properties.put("zookeeper.connect", "172.20.104.92:2181");
		properties.put("group.id", "group-10-27");
		properties.put("auto.offset.reset", "largest");
		return Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
	}
	
	/**
	 * to judge whether the table exist
	 * @param tablename
	 * @throws MasterNotRunningException
	 * @throws ZooKeeperConnectionException
	 * @throws IOException
	 */
	public void isExitsTable(String tablename) throws MasterNotRunningException, ZooKeeperConnectionException, IOException{
		HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
		if (hBaseAdmin.tableExists(tablename)) {
			hBaseAdmin.disableTable(tablename);
			hBaseAdmin.deleteTable(tablename);
			System.out.println(tablename + " is exist,detele....");
		}
		HTableDescriptor tableDescriptor = new HTableDescriptor(tablename);
		tableDescriptor.addFamily(new HColumnDescriptor("result"));
		hBaseAdmin.createTable(tableDescriptor);
	}
	/**
	 * inser into hbase 
	 * @param tableName
	 * @param list<put>
	 * @throws MasterNotRunningException
	 * @throws ZooKeeperConnectionException
	 * @throws IOException
	 */
	public void insert(String tableName,List<Put> list) throws MasterNotRunningException, ZooKeeperConnectionException, IOException{
		HTable table = new HTable(conf, tableName);
		table.put(list);
		table.close();
	}
	
	/**
	 * to main function to get data from kafka block and insert into hbase
	 */
	public void run() {
		ConsumerConnector consumer = createConsumer();
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, 1); 
		Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumer.createMessageStreams(topicCountMap);
		KafkaStream<byte[], byte[]> stream = messageStreams.get(topic).get(0);
		ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
		try {
			SimpleDateFormat df = new SimpleDateFormat("yyyy_MM_dd");
			String Start = df.format(new Date());			
			String tablename = "gongguiweiKafa";
			List<Put> list =new ArrayList();
			isExitsTable(tablename);
			System.out.println("读表成功！");
			System.out.println("写表的状态"+Start+"等于"+df.format(new Date()));
			int count =0;
			System.out.println("准备进入！");

			while (iterator.hasNext()) {
				System.out.println("进入循环，读取kafka");

				String message = new String(iterator.next().message());
				String s = message.toString();//get the message				
				String rowkey2 = s;
				Put put = new Put(Bytes.toBytes(rowkey2));
				put.add(Bytes.toBytes("result"), Bytes.toBytes("Value"), Bytes.toBytes(s+"1234567"));				
				list.add(put);		
				count++;
				if(count%10==0){
					System.out.println("插入hbase！");

					insert(tablename,list);
				}
								
			}
			System.out.println("表关闭于"+new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("No data to read!");

		}
	}

	

	public static void main(String[] args) {
		new InsetHbase("test").run();//start run function 
	}
}
