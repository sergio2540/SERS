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
	
	public static byte[] getSHA1(String username) {
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
	
    final protected static char[] hexArray = "0123456789ABCDEF".toCharArray();
	
	public static String bytesToHex(byte[] bytes) {
	    char[] hexChars = new char[bytes.length * 2];
	    int v;
	    for ( int j = 0; j < bytes.length; j++ ) {
	        v = bytes[j] & 0xFF;
	        hexChars[j * 2] = hexArray[v >>> 4];
	        hexChars[j * 2 + 1] = hexArray[v & 0x0F];
	    }
	    return new String(hexChars);
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
		
		if(args[0] == null && args[1] == null) {
			System.out.println("Argumentos inválidos!");
			return;
		}
		
		MyKey usersKey = new MyKey("/admin/usersKey");
		MyKey activeUsersKey = new MyKey("/admin/activeUsersKey");
		MyKey duplicatedActiveUsersKey = new MyKey("/admin/duplicatedActiveUsersKey");
		
		String username = args[0];
		String folder = args[1];
		
		System.out.println("USERNAME: " + "|" + args[0] + "|");
		System.out.println("FOLDER: " + "|" + args[1] + "|");

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

		MyKey keyRoot = new MyKey("/");
		
		if(username.equals("admin")) {
	
			
				System.out.println("ADMIN CREATED");
				String usersContent = username;
				String rootContent = "DIR\n";
				//chord.insert(keyRoot, rootContent);
				//chord.insert(usersKey, usersContent);
				System.out.println("Insert executed!");
			}
		
		for(URL url : peersList) {
			try {
				
				System.out.println("LOCALUTL: " + localURL + " , URL: " + url);
				System.out.println("users");
				chord.join(localURL, new ID(getSHA1(mac)), url);

				try {
				
						chord.insert(usersKey, username);
						
						Set<Serializable> usersData = chord.retrieve(usersKey);
						StringBuilder users = new StringBuilder();
						System.out.println("number of elements in users data: " + usersData.size());
						for(Serializable ser : usersData) {
							System.out.println("number of users: " + i);
							users.append(ser.toString());
							users.append("\n");
						}
						
						System.out.println(users.toString());
						
					
				
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
						
						
						System.out.println("Retrieve:------------"+s);

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
				System.out.println("Create executed!");
				peersFile.prependPeerToPeerList(localURL);
				peersFile.save();

				return;
				
			} catch (ServiceException e) {
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
		
		Fs fs = Fs.initializeFuse(chord, folder, false);
		
		
	}

}
