package SEPRS;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import de.uniba.wiai.lspi.chord.console.command.entry.Key;
import de.uniba.wiai.lspi.chord.data.URL;
import de.uniba.wiai.lspi.chord.service.Chord;
import de.uniba.wiai.lspi.chord.service.PropertiesLoader;
import de.uniba.wiai.lspi.chord.service.ServiceException;
import de.uniba.wiai.lspi.chord.service.impl.ChordImpl;

public class Client {

	
	static void getFileFromUrl(){
		
		
		URL website = null;
		try {
			website = new URL("https://dl.dropboxusercontent.com/u/23827391/peers");
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
		ReadableByteChannel rbc = null;
		try {
			rbc = Channels.newChannel(website.);
		
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		FileOutputStream fos = null;
		try {
			fos = new FileOutputStream("peers");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		try {
			fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	public static void main(String[] args){
		
		InputStream    fis;
		BufferedReader br;
		String         line;
		List<URL> addressList = new ArrayList<URL>();
		String userHome = System.getProperty("user.home");
		fis = new FileInputStream(userHome);
		br = new BufferedReader(new InputStreamReader(fis, Charset.forName("UTF-8")));
		String protocol = URL.KNOWN_PROTOCOLS.get(URL.SOCKET_PROTOCOL);

		PropertiesLoader.loadPropertyFile();

		URL localURL = null;
		
				try{
					
					localURL = new URL(protocol + "://localhost:8181/");
					
				}catch(MalformedURLException e){
					//throw new RuntimeException(e);
				}
		
		while ((line = br.readLine()) != null) {

			try{
			addressList.add(new URL(protocol + "://" + line + ":8080/"));
			}catch(MalformedURLException e)
			{
				throw new RuntimeException(e);
				
			}	
		}

		// Done with the file
		br.close();
		br = null;
		fis = null;
				
		
		Chord chord = new ChordImpl();
		try{
			
			chord.join(localURL, bootstrapURL);
		}catch(ServiceException e){
			
			throw new RuntimeException("Could not join DHT!");
		}
		
		Key sk = new Key("Joke");
		try {
			chord.insert(sk, "Article about jokes!!!");
		} catch (ServiceException e) {
			throw new RuntimeException(e);
		}
		
		try {
			Set<Serializable> set = chord.retrieve(sk);
			Iterator<Serializable> it = set.iterator();
			while(it.hasNext())
			{
				
				String s = (String) it.next();
				System.out.println(s);
				
			}
			
		} catch (ServiceException e) {
			throw new RuntimeException(e);
		}
	}
	
}
