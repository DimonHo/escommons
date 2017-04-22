package com.dimonho.es.commons;

import java.lang.reflect.Constructor;

import org.apache.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

/**
 * ES客户端工厂
 * @author dimonho
 *
 */
public class ClientFactory{
	
	private static final Logger LOGGER = Logger.getLogger(ClientFactory.class);
	
	/**
	 * ES请求地址
	 */
	private String address;
	
	/**
	 * ES集群名字
	 */
	private String clusterName;
	
	private TransportClient client;
	
	public static final int DEFAULT_ES_PORT=9300;
	
	/**
	 * 初始化方法，使用配置的参数来构建一个客户端
	 */
	public void init(){
		LOGGER.info(String.format("初始化ES链接:%s(%s)",address,clusterName));
		
		Settings defaultSettings = ImmutableSettings.settingsBuilder()
				.put("client.transport.sniff", false)
				.put("client.transport.ignore_cluster_name", true)
				.put("index.similarity.default.type", "default")
				.put("cluster.name",clusterName)
				.build();
		try {
			Class<?> clazz = Class.forName(TransportClient.class.getName());
			Constructor<?> constructor = clazz.getDeclaredConstructor(Settings.class);
			constructor.setAccessible(true);
			client = (TransportClient) constructor.newInstance(defaultSettings);
			String[] addrs = address.split(",");
			for(String str : addrs){
				String[] items = str.split(":");
				if(items.length==2){
					client.addTransportAddress(new InetSocketTransportAddress(items[0], Integer.valueOf(items[1])));
				}else if(items.length ==1){
					client.addTransportAddress(new InetSocketTransportAddress(items[0], DEFAULT_ES_PORT));
				}
			}
		} catch (Exception e) {
			throw new ESConnectionException(e);
		}
	}

	/**
	 * 取得实例
	 * @return
	 */
	public  Client getTransportClient() {
		return client;
	}
	
	public Client getTransportClient(String clusterName,String ip,Integer port){
		return createClient(clusterName,ip,port);
	}
	
	/**
	 * 创建一个客户端
	 * @param clusterName
	 * @param ip
	 * @param port
	 * @return
	 */
	public Client createClient(String clusterName,String ip,Integer port){
		Settings settings = ImmutableSettings.settingsBuilder()
				.put("client.transport.sniff", false)
				.put("client.transport.ignore_cluster_name", true)
				.put("index.similarity.default.type", "default")
				.put("cluster.name",clusterName)
				.build();
		return createClient(settings,ip,port);
	}
	
	public Client createClient(Settings settings,String ip,Integer port){
		TransportClient client =  new TransportClient(settings);
		client.addTransportAddress(new InetSocketTransportAddress(ip,port));
		return client;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public String getClusterName() {
		return clusterName;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

}
