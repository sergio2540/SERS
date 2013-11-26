package chord;

import de.uniba.wiai.lspi.chord.service.*;

public class MyKey implements Key {

	private String key;
	
	public MyKey(String key) {
		this.key = key;
	}
	
	@Override 
	public int hashCode() {
		return this.key.hashCode();		
	}
	
	@Override
	public boolean equals(Object key) {
		return this.key.equals(key);		
	}
	
	@Override
	public byte[] getBytes() {
		return key.getBytes();
	}
	
}
