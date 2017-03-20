package com.gong.kafka;

import java.io.UnsupportedEncodingException;
import java.util.List;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class MessageScheme implements Scheme {

	public List<Object> deserialize(byte[] arg0) {
		try {
			String msg = new String(arg0, "UTF-8");
			String msg_0 = "hello";
			return new Values(msg_0, msg);
		} catch (UnsupportedEncodingException e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		return null;
	}

	
	public Fields getOutputFields() {
		// TODO Auto-generated method stub
		return  new Fields("key","message");
	}

}
