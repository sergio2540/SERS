package chord;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.MalformedURLException;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

import de.uniba.wiai.lspi.chord.data.ID;
import de.uniba.wiai.lspi.chord.data.URL;
import de.uniba.wiai.lspi.chord.service.Chord;
import de.uniba.wiai.lspi.chord.service.PropertiesLoader;
import de.uniba.wiai.lspi.chord.service.ServiceException;
import de.uniba.wiai.lspi.chord.service.impl.ChordImpl;
import fuse.Fs;

public class Client {

	static int N = 15;
	static int HIGHPORT = 49151;
	static int LOWPORT = 1024;
	
	private static byte[] getSHA1(String username) {
		String sha1;
		
		MessageDigest mDigest = null;
		try {
			mDigest = MessageDigest.getInstance("SHA1");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		byte[] result = mDigest.digest(username.getBytes());
		
		return result;
		
	}
	
	private static String getMacAdress() {

		InetAddress ip;
	 
		StringBuilder sb = null;
		
			try {
				
			ip = InetAddress.getLocalHost();
			
			//System.out.println("Current IP address : " + ip.getHostAddress());
			
			Enumeration<NetworkInterface> nets = NetworkInterface.getNetworkInterfaces();
			NetworkInterface netint;
			List<InterfaceAddress> interfaces = new ArrayList<InterfaceAddress>();
			
			while(nets.hasMoreElements()) {
				 netint = nets.nextElement();
				 interfaces.addAll(netint.getInterfaceAddresses());
			}

			//for(InterfaceAddress intAddr : interfaces){
			//	System.out.println("| " + intAddr.toString() + " |");
				
			//}
			
			ip = InetAddress.getByAddress(interfaces.get(1).getAddress().getAddress());
			NetworkInterface network = NetworkInterface.getByInetAddress(ip);
	 
			if(network == null) {
				System.out.println("NULL");
				return "";
			}
			
			byte[] mac = network.getHardwareAddress();
	 
			sb = new StringBuilder();
			
			for (int i = 0; i < mac.length; i++) {
				sb.append(String.format("%02X%s", mac[i], (i < mac.length - 1) ? "-" : ""));		
			}
			
			//System.out.println(sb.toString());
			
			} catch (UnknownHostException | SocketException e) {
				e.printStackTrace();
			}
		
		return sb.toString();
		
	}
	
	
	private static int generateRandomPort(int higher, int lower) {
		
		Random random = new Random();
		//numero entre 1 e higher
		int port = lower + random.nextInt(higher - lower) + 1;
		//Debug
		//System.out.println("lower: " + lower + " higher: " + higher + " num: " + num);
		System.out.println("Random port: " + port);
		return port;
		
	}
	

	public static void main(String[] args){

		InputStream    fis = null;
		BufferedReader br;
		String         line;
		List<URL> addressList = new ArrayList<URL>();
		String userHome = System.getProperty("user.home");
		Collection<URL> peersList;
		Collection<URL> peersToBeRemoved = new ArrayList<URL>();

		PeersFile peersFile = new PeersFile("https://dl.dropboxusercontent.com/u/23827391/peers", "peers");
		peersList = peersFile.getPeersList();

		PropertiesLoader.loadPropertyFile();
		int countFailedChordConnections = 0;
		
		Chord chord = new ChordImpl();
		
		System.out.println(getMacAdress());
		
		String protocol = URL.KNOWN_PROTOCOLS.get(URL.SOCKET_PROTOCOL);
		System.out.println("PROTOCOL: " + protocol.toString());
		URL localURL = null;

		try {

			localURL = new URL(protocol + "://localhost:" + generateRandomPort(HIGHPORT, LOWPORT) + "/");
			
		} catch(MalformedURLException e){
			//throw new RuntimeException(e);
		}

		//pode devolver NULL
		String mac = getMacAdress(); //concatenar com username
		
		if(peersList.isEmpty()) {
			System.out.println("PeersList empty!");
		}
		
		for(URL url : peersList) {
			try {
				
				System.out.println("LOCALUTL: " + localURL + " , URL: " + url);
				chord.join(localURL, new ID(getSHA1(mac)), url);
				
				MyKey keyRoot = new MyKey("/");
//				MyKey keyHello = new MyKey("/hello.txt");
//				MyKey key0 = new MyKey("0");
				
//				System.out.println("KEY BYTES: " + keyRoot.getBytes().length);
				
//				ID id = chord.getID();
//				System.out.println("ID DO CHORD: "  +  id);
//				System.out.println("ID DO CHORD: "  +  id.getLength());

				try {
					
					String rootContent = "DIR\n";
//					System.out.println("content " + rootContent.getBytes().length);
					
					chord.insert(keyRoot, rootContent);
					
//					String helloContent = "FILE\n" + "Hello world!".length() + "\n0";
//					System.out.println("content " + helloContent.getBytes().length);
					
					//chord.insert(keyHello, helloContent);
					
//					String zeroContent = "Hello world!";
//					System.out.println("content " + zeroContent.getBytes().length);
					
//					chord.insert(key0, zeroContent);
				
				} catch (Exception e) {
					System.out.println(e.getMessage());
					throw new RuntimeException(e);
				}

				try {
					Set<Serializable> set = chord.retrieve(keyRoot);
					Iterator<Serializable> it = set.iterator();
					
					while(it.hasNext())
					{

						String s = (String) it.next();
						
						
						System.out.println("Retrieve:---------------------------------"+s);

					}

				} catch (ServiceException e) {
					throw new RuntimeException(e);
				}
				
				break;
				
			} catch (ServiceException e) {
				countFailedChordConnections++; //ja nao estamos a usar
				peersToBeRemoved.add(url);
			}

		}
		
		System.out.println("FAILS JOIN: " + countFailedChordConnections);
		
		peersFile.removePeersInPeersList(peersToBeRemoved);
		int alivePeers = peersList.size();
		
		System.out.println("ALIVE VARIABLE: " + alivePeers);
		
		if(alivePeers == 0){
			
			try {
				chord.create(localURL, new ID(getSHA1(mac)));
			} catch (ServiceException e) {
				System.out.println("h");
				System.out.println(e.getMessage());
			
			}
			
		} else {
			
			
		}
		
		peersFile.prependPeerToPeerList(localURL);
		
		peersFile.save();
		
		
		//verfica se tem que ir buscar mais ips ao "DNS"
		if((2/3) * N > alivePeers) {
				
			
			//chama funcao que faz download do ficheiro peers global
		}
		
		Fs fs = Fs.initializeFuse(chord, "/home/pedro/Desktop/fuse3" , true);
		
	}

}
