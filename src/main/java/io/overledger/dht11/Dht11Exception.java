package io.overledger.dht11;

public class Dht11Exception extends Exception {
	
	private static final long serialVersionUID = 1L;
	private final String message;
	
	public Dht11Exception(String message) {
		this.message = message;
	}
	
	public String getMessage() {
		return this.message;
	}

}
