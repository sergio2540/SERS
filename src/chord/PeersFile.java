package chord;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Collection;

import de.uniba.wiai.lspi.chord.data.URL;

public class PeersFile {

	String url;
	String path;
	File file;
	Collection<URL> peersList;


	public PeersFile(String url, String path) {
		this.url = url;
		this.path = path;
		this.file = new File(path);

		if(!fileExists()){
			getFileFromUrl();
			read();
		} else {
			read();
		}

	}



	public PeersFile(String path) {
		this.path = path;
		this.file = new File(path);

		if(!fileExists()){
			getPeersFromDB();
			read();
		} else {
			read();
		}

	}
	
	public void getPeersFromDB(){

		HbaseManager manager = new HbaseManager();
		manager.prepareDB();
		String urls = manager.get();
		
		if(urls == null) {
			System.out.println("URLS = NULL");
			return;
		}

		String[] splittedUrls = urls.split(" ");

		FileOutputStream fos = null;
		try {
			fos = new FileOutputStream(this.path);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		for(String toWrite : splittedUrls) {

			try {

				fos.write((toWrite + "\n").getBytes());
				fos.close();

			} catch (IOException e) {
				System.out.println("Eror while writing to peers file " + e.getMessage());
			}

		}
		
		try {
			manager.cleanUp();
		} catch (IOException e) {
		}


	}


	public void getFileFromUrl() {

		java.net.URL website = null;
		try {
			website = new java.net.URL(this.url);
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
		ReadableByteChannel rbc = null;
		try {
			rbc = Channels.newChannel(website.openStream());
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		FileOutputStream fos = null;
		try {
			fos = new FileOutputStream(this.path);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		try {
			fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public File getPeersFile() {
		return file;
	}

	public boolean deletePeersFile() {
		return file.delete();
	}

	private boolean fileExists() {
		return file.exists();
	} 

	public void prependPeersOnFile(Collection<String> newPeers) {

		FileWriter fileWriter;

		try {
			fileWriter = new FileWriter(file, false);

			for(String peer : newPeers) {
				fileWriter.write(peer);
			}

			fileWriter.close();

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public void appendPeersOnFile(Collection<String> newPeers) {

		FileWriter fileWriter;

		try {

			fileWriter = new FileWriter(file, true);

			for(String peer : newPeers) {
				fileWriter.write(peer);
			}

			fileWriter.close();

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public void read() {

		File file = getPeersFile();
		FileReader fileReader = null;
		BufferedReader bufferedReader = null;
		Collection<URL> urls = new ArrayList<URL>();

		try {

			fileReader = new FileReader(file);
			bufferedReader = new BufferedReader(fileReader);
			String line;

			while((line = bufferedReader.readLine()) != null){

				System.out.println("URL: " + "|" + line + "|");
				urls.add(new URL(line));

			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}	finally {

			try {
				if(fileReader != null) {
					fileReader.close();
				} 
				if(bufferedReader != null) {
					bufferedReader.close();
				}			
			} catch (IOException e) {
				e.printStackTrace();
			}

		}

		peersList = urls;

	}

	public Collection<URL> getPeersList() {
		return this.peersList;
	}

	//retorna true se sucesso e retorna false se nao teve
	public boolean removePeersInPeersList(Collection<URL> urlsToRemove) {
		return peersList.removeAll(urlsToRemove);
	}

	//retorna true se sucesso e retorna false se nao teve
	public boolean appendPeersToPeersList(Collection<URL> urlsToAdd) {
		return peersList.addAll(urlsToAdd);
	}

	public boolean appendPeerToPeersList(URL url) {
		return peersList.add(url);
	}

	public boolean removePeerInPeersList(URL url) {
		return peersList.remove(url);
	}

	public void prependPeerToPeerList(URL peer) {

		Collection<URL> temp = new ArrayList<URL>();
		temp.add(peer);
		temp.addAll(peersList);
		peersList = temp;

	}


	public void prependPeersToPeerList(Collection<URL> peers) {
		peers.addAll(peersList);
		peersList = peers;
	}	

	public void save() {

		File file = getPeersFile();
		FileWriter fileWriter = null;
		BufferedWriter bufferedWriter = null;

		try {

			fileWriter = new FileWriter(file);
			bufferedWriter = new BufferedWriter(fileWriter);

//			if(peersList.size() == 0)
//				bufferedWriter.write("");
				
			for(URL url : peersList) {

				bufferedWriter.write(url.toString() + "\n");

			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}	finally {

			try {
				if(bufferedWriter != null) {
					bufferedWriter.close();
				}		
				if(fileWriter != null) {
					fileWriter.close();
				} 
			} catch (IOException e) {
				e.printStackTrace();
			}

		}

	}

}
