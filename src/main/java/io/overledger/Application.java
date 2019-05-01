package io.overledger;

import com.pi4j.io.gpio.RaspiPin;
import io.overledger.blockchain.ContractWriter;
import io.overledger.dht11.Data;
import io.overledger.dht11.Dht11;
import java.math.BigInteger;

public class Application implements Runnable {

    private ContractWriter contractWriter = ContractWriter.newInstance(Thread.currentThread().getContextClassLoader().getResourceAsStream("context.properties"));

    public Application() {
        new Thread(this).start();
    }

    @Override
    public void run() {
        while (true) {
        	System.out.println("Start data capturing ...");
            Data data = Dht11.getDht11Data(RaspiPin.GPIO_07);
            System.out.println("Data received");
            BigInteger timestamp = new BigInteger(Long.toString(System.currentTimeMillis()));
            try {
                String txHashList[] = this.contractWriter.sendData(
                		new String[]{"celsius", "humidity"}, 
                		new BigInteger[]{data.getCelsius(), data.getHumidity()}, 
                		new BigInteger[]{timestamp, timestamp});
                System.out.println("TXN: " + txHashList[0] + " - CELSIUS: " + data.getCelsius() + " at: " + timestamp);
                System.out.println("TXN: " + txHashList[1] + " - HUMIDITY: " + data.getHumidity() + " at: " + timestamp);
                synchronized (this) {
                    this.wait(3600000);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        new Application();
    }

}
