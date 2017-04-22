package com.dimonho.es.commons;

/**
 * ES链接异常
 * @author dimonho
 *
 */
public class ESConnectionException extends RuntimeException{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public ESConnectionException(){
		super();
	}
	
	public ESConnectionException(String msg){
		super(msg);
	}
	
	public ESConnectionException(Throwable t){
		super(t);
	}
	
	public ESConnectionException(String msg,Throwable t){
		super(msg,t);
	}

}
