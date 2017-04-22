package com.dimonho.es.commons;

import java.util.Map;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.junit.Before;
import org.junit.Test;

/**
 * 数据迁移测试
 * @author Administrator
 *
 */
public class MoveDataTest {

	private String sourceClusterName = "our_es";
	
	private String targetClusterName = "our_es";
	//源索引
	private String sourceIndex = "wos_source";
	//目标索引
	private String targetIndex = "wos_source";
	
	private String sourceType = "periodical";
	
	private String targetType = "periodical";

	private Client sourceClient;

	private Client targetClient;
	
	private static int errcount = 0;
	
	@Before
	public void before(){
		ClientFactory clientFactory = new ClientFactory();
		sourceClient = clientFactory.getTransportClient(sourceClusterName, "192.168.1.75", 9300);
		targetClient = clientFactory.getTransportClient(targetClusterName, "www.dimonho.com", 9300);
		
	}
	
	@Test
	public void bulkMove(){
		QueryBuilder queryBuilder = QueryBuilders.matchAllQuery();
		SearchResponse sourceDatas = sourceClient.prepareSearch(sourceIndex)
				.setTypes(sourceType)
				.setQuery(queryBuilder)
				.setSize(800)
				.setScroll(TimeValue.timeValueSeconds(2))
				.setSearchType(SearchType.SCAN)
				.execute()
				.actionGet();
		long sumCount = sourceDatas.getHits().getTotalHits();
		System.out.println("========共查询出"+sumCount+"条数据===========");
		
		String scrollId = sourceDatas.getScrollId();
		System.out.println("scrollId:"+scrollId);
		BulkRequestBuilder bulkRequest = getBulkRequestBuilder();
		
		while(true){
			SearchResponse scrollResponse = sourceClient.prepareSearchScroll(scrollId)
					.setScroll(TimeValue.timeValueMinutes(8)).execute().actionGet();
			SearchHit[] hits = scrollResponse.getHits().hits();
			int count = hits.length;
			sumCount -= count;
			if(count == 0){
				break;
			}
			for(SearchHit hit:hits){
				Map<String,Object> source = hit.getSource();
				source.put("journalName", hit.getSource().get("journal"));
				IndexRequestBuilder indexRequesBuider = getIindexRequesBuider();
				if (source!=null && null != source.get("_id")) {
					String id = (String)source.remove("_id");
					
					indexRequesBuider.setId(id);
				}
				indexRequesBuider.setOpType(IndexRequest.OpType.CREATE);
				bulkRequest.add(indexRequesBuider.setSource(source));
			}
			insert(bulkRequest);
			
			bulkRequest = getBulkRequestBuilder();
			System.out.println("还剩"+sumCount+"条数据待导入。。。。。。。。。。。。。。。");
			
		}
		
	}

	private void insert(BulkRequestBuilder bulkRequest) {
		try{
			//如果中途失败，bulkRequest可能还有值未完全插入，需要检测是否有值再重新执行插入。
			if (bulkRequest.request().requests().size() != 0){
				//BulkResponse接收失败信息
				bulkRequest.execute().actionGet();
//				if(bulkResponse.hasFailures()){
//					int succes = 0;
//					for(BulkItemResponse item : bulkResponse.getItems()){
//						if(item.isFailed()){
//							errcount++;
//						}else{
//							succes++;
//						}
//					}
//					System.out.println();
//					System.out.println("失败"+errcount+"条\n"+bulkRequest.request().requests().size()+"》》成功:"+succes+"条");
//				}
			}
		}catch(Exception e){
			e.printStackTrace();
			System.out.println("连接目标服务器失败！重新连接。。。。");
			try {
				Thread.sleep(10000);
				insert(bulkRequest);//防止出现链接超时现象，自动重新运行。
			} catch (InterruptedException e1) {
				e1.printStackTrace();
				insert(bulkRequest);//防止出现链接超时现象，自动重新运行。
			}
			
		}
	}
	
	public IndexRequestBuilder getIindexRequesBuider(){
		try{
			return targetClient.prepareIndex(targetIndex, targetType);
		}catch(Exception e){
			e.printStackTrace();
			System.out.println("获取IindexRequesBuider实例失败，重新获取。。。。。");
			try {
				Thread.sleep(10000);
				return getIindexRequesBuider();
			} catch (InterruptedException e1) {
				e1.printStackTrace();
				return getIindexRequesBuider();
			}
			
		}
	}
	
	public BulkRequestBuilder getBulkRequestBuilder(){
		try {
			return targetClient.prepareBulk();
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("获取BulkRequestBuilder实例失败，重新获取。。。。。");
			try {
				Thread.sleep(10000);
				return getBulkRequestBuilder();
			} catch (InterruptedException e1) {
				e1.printStackTrace();
				return getBulkRequestBuilder();
			}
		}
	}
}
