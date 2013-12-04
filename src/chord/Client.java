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

import de.uniba.wiai.lspi.chord.console.command.Retrieve;
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

		if(args[0] == null && args[1] == null) {
			System.out.println("Invalid arguments!");
			System.exit(1);
		}
		
		Collection<URL> peersList;
		Collection<URL> peersToBeRemoved = new ArrayList<URL>();

		//keys and contents
		MyKey keyRoot = new MyKey("/");
		String keyRootContent = "DIR\n";
		
		//MyKey usersKey = new MyKey("/admin/usersKey");
		//MyKey activeUsersKey = new MyKey("/admin/activeUsersKey");
		//MyKey duplicatedActiveUsersKey = new MyKey("/admin/duplicatedActiveUsersKey");
		
		//keys and contents
		
		//username and mounting point
		
		
		PropertiesLoader.loadPropertyFile();
		
		String username = args[0];
		String folder = args[1];
		//username and mounting point

		PeersFile peersFile = new PeersFile("https://dl.dropboxusercontent.com/u/23827391/peers", "peers");
		peersList = peersFile.getPeersList();

		Chord chord = new ChordImpl();
		String protocol = URL.KNOWN_PROTOCOLS.get(URL.SOCKET_PROTOCOL);
		URL localURL = null;

		try {
			//TODO: trocar localhost para ip da maquina
			localURL = new URL(protocol + "://localhost:" + generateRandomPort(HIGHPORT, LOWPORT) + "/");

		} catch(MalformedURLException e) {
			System.out.println(e.getMessage());
			System.exit(1);
		}

		//pode devolver NULL
		String mac = getMacAdress();
		
		if(mac == null) {
			System.out.println("Could not create your ID because we couldnt get your MAC address");
			System.exit(1);
		}

		
		if(username.equals("superadmin")) {
			
			try {
				chord.create(localURL, new ID(getSHA1(mac)));
				System.out.println("Create executed!");
				peersFile.prependPeerToPeerList(localURL);
				peersFile.save();
				while(true);
			} catch (ServiceException e) {
				e.printStackTrace();
			}

		}
		
		if(peersList.isEmpty()) {
			System.out.println("PeersList empty!");
		}

		for(URL url : peersList) {
			try {

				chord.join(localURL, new ID(getSHA1(mac)), url);

				if(username.equals("admin")) {
					
					boolean firstTime = false;
					Set<Serializable> data = chord.retrieve(keyRoot);
					firstTime = data.isEmpty();

					if(firstTime) {
						chord.insert(keyRoot, keyRootContent);

					}
				
//						Set<Serializable> usersData = chord.retrieve(usersKey);
//						StringBuilder users = new StringBuilder();
//						System.out.println("number of elements in users data: " + usersData.size());
//						for(Serializable ser : usersData) {
//							users.append(ser.toString());
//							users.append("\n");
//						}
//						System.out.println(users.toString());
				}
				

				break; //sai da lista de peers se conseguir fazer join

			} catch (ServiceException e) {
				peersToBeRemoved.add(url);
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}

		}

		peersFile.removePeersInPeersList(peersToBeRemoved);
		int peersInList = peersList.size();
		System.out.println("PEERS IN THE LIST OF PEERS" + peersInList);

		peersFile.prependPeerToPeerList(localURL);
		peersFile.save();

		//verfica se tem que ir buscar mais ips ao "DNS"
		if((2/3) * N > peersInList) {


			//chama funcao que faz download do ficheiro peers global
		}

		Fs fs = Fs.initializeFuse(chord, folder, false);


	}

}
