package gossip;

import java.util.Set;

import de.uniba.wiai.lspi.chord.com.CommunicationException;
import de.uniba.wiai.lspi.chord.com.Entry;
import de.uniba.wiai.lspi.chord.com.Node;
import de.uniba.wiai.lspi.chord.com.Proxy;
import de.uniba.wiai.lspi.chord.data.URL;
import de.uniba.wiai.lspi.chord.service.Chord;
import de.uniba.wiai.lspi.chord.service.impl.ChordImpl;
import de.uniba.wiai.lspi.chord.service.impl.NodeImpl;

public class Client {

	Chord chord;
	
	URL localUrl;
	URL bootstrapUrl;
	
	
	public Client(Chord chord) {
		this.chord = chord;
		
		//this.localUrl = localUrl;
		//this.bootstrapUrl = bootstrapUrl;
	}
	
	public void getEntriesById() {
		
		/*
		ChordImpl chordImpl = (ChordImpl) chord;  
		String s = chordImpl.printEntries();
		
		String[] splittedLines = s.split("\n");
		int i = 0;
		for(String line : splittedLines){
			
			System.out.println("Numero da linha " + i + ":" + line + "|");
			i++;
		}
		*/
		
		
		/*
		Node node;
		try {
		
			node = Proxy.createConnection(bootstrapUrl, localUrl);
			Set<Entry> entries;
			entries = node.retrieveEntries(this.chord.getID());
			
			int filesSize = 0;
			
			for(Entry e: entries){
				
				filesSize += e.getValue().toString().getBytes().length;
			
			}
			
			System.out.println("filesSize" + filesSize);
		
		} catch (CommunicationException e1) {
			e1.printStackTrace();
		}
	
	*/
		
	}
	
}
