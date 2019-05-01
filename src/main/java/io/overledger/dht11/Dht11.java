package io.overledger.dht11;

import java.math.BigDecimal;
import java.math.BigInteger;

import com.pi4j.io.gpio.Pin;
import com.pi4j.io.gpio.RaspiPin;
import com.pi4j.wiringpi.Gpio;
import com.pi4j.wiringpi.GpioUtil;

public final class Dht11 {
	
	private final static int MAXTIMINGS = 0x55;
	private static STATUS DEV_STATUS = STATUS.UNKNOWN;
	
	private Dht11() {}
	
	private static boolean checkParity(int dht11Data[]) {
        return dht11Data[4] == (dht11Data[0] + dht11Data[1] + dht11Data[2] + dht11Data[3] & 0xFF);
    }
	
	public static Data getData(final Pin pin) throws Dht11Exception {
		if (null == pin) {
			throw new Dht11Exception("Unknown pin number");
		}
		if (STATUS.UNKNOWN == DEV_STATUS) {
			DEV_STATUS = (-1 == Gpio.wiringPiSetup())?STATUS.FAIL: STATUS.SETUP;
			return getData(pin);
		} else if (STATUS.FAIL == DEV_STATUS) {
			throw new Dht11Exception("Fail to setup GPIO");
		} else if (STATUS.SETUP == DEV_STATUS) {
			GpioUtil.export(3, GpioUtil.DIRECTION_OUT);
			DEV_STATUS = STATUS.READY;
		}
		
		int address = pin.getAddress();
		int lastState = Gpio.HIGH;
		int dht11Data[] = { 0, 0, 0, 0, 0 };
		int index = 0;
		
		Gpio.pinMode(address, Gpio.OUTPUT);
		Gpio.digitalWrite(address, Gpio.LOW);
		Gpio.delay(18);
		Gpio.digitalWrite(address, Gpio.HIGH);
		Gpio.pinMode(address, Gpio.INPUT);
		
		for (int i=0; i<MAXTIMINGS; i++) {
			int counter = 0;
			while (lastState == Gpio.digitalRead(address)) {
				counter++;
				Gpio.delayMicroseconds(1);
				if (0xFF == counter) {
					break;
				}
			}
			lastState = Gpio.digitalRead(address);
			if (0xFF == counter) {
				break;
			}
			if (i >= 4 && i % 2 == 0) {
				dht11Data[index / 8] <<= 1;
				if (counter > 16) {
					dht11Data[index / 8] |= 1;
				}
				index++;
			}
			
		}
		
		if (index >= 40 && checkParity(dht11Data)) {
            float h = (float) ((dht11Data[0] << 8) + dht11Data[1]) / 10;
            if (h > 100) {
                h = dht11Data[0];
            }
            float c = (float) (((dht11Data[2] & 0x7F) << 8) + dht11Data[3]) / 10;
            if (c > 125) {
                c = dht11Data[2];
            }
            if ((dht11Data[2] & 0x80) != 0) {
                c = -c;
            }
            Data data = new Data();
            data.setHumidity(BigInteger.valueOf((int)(h*100)));
            data.setCelsius(BigInteger.valueOf((int)(c*100)));
			data.setFahrenheit(BigInteger.valueOf((int)((c*1.8+32)*100)));
            return data;
        } else {
        	throw new Dht11Exception("Broken data...");
        }
	}
	
	public static Data getDht11Data(final Pin pin, final int maxAttempt) {
		Data data = null;
		int attempt = 0;
		while (null == data && attempt++ < maxAttempt) {
            try {
            	data = Dht11.getData(pin);
            } catch(Dht11Exception e) {
            	data = null;
            }
		}
		return data;
	}
	
	public static Data getDht11Data(final Pin pin) {
		Data data = null;
		while (null == data) {
            try {
            	data = Dht11.getData(pin);
            } catch(Dht11Exception e) {
            	data = null;
            }
		}
		return data;
	}

}
