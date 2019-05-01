package io.overledger.dht11;

import java.math.BigInteger;

public class Data {

	private BigInteger humidity;
	private BigInteger celsius;
	private BigInteger fahrenheit;

	public BigInteger getHumidity() {
		return humidity;
	}

	public void setHumidity(BigInteger humidity) {
		this.humidity = humidity;
	}

	public BigInteger getCelsius() {
		return celsius;
	}

	public void setCelsius(BigInteger celsius) {
		this.celsius = celsius;
	}

	public BigInteger getFahrenheit() {
		return fahrenheit;
	}

	public void setFahrenheit(BigInteger fahrenheit) {
		this.fahrenheit = fahrenheit;
	}

	@Override
	public String toString() {
		return "Data [humidity=" + humidity + ", celsius=" + celsius
				+ ", fahrenheit=" + fahrenheit + "]";
	}
	
	

}
