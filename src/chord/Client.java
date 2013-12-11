package chord;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.MalformedURLException;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import de.uniba.wiai.lspi.chord.com.Entry;
import de.uniba.wiai.lspi.chord.com.Node;
import de.uniba.wiai.lspi.chord.console.command.Retrieve;
import de.uniba.wiai.lspi.chord.data.ID;
import de.uniba.wiai.lspi.chord.data.URL;
import de.uniba.wiai.lspi.chord.service.Chord;
import de.uniba.wiai.lspi.chord.service.PropertiesLoader;
import de.uniba.wiai.lspi.chord.service.ServiceException;
import de.uniba.wiai.lspi.chord.service.impl.ChordImpl;
import fuse.Fs;
import gossip.core.Gossip;
import gossip.message.*;

public class Client {

	static int N = 15;
	static int HIGHPORT = 49151;
	static int LOWPORT = 1024;

	private static Chord chord;

	private static DatagramSocket socket;

	public static byte[] getSHA1(byte[] username) {
		String sha1;

		MessageDigest mDigest = null;
		try {
			mDigest = MessageDigest.getInstance("SHA1");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		byte[] result = mDigest.digest(username);

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

	private static byte[] getMacAdress() {

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
				return "".getBytes();
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

		return sb.toString().getBytes();

	}

	private static int generateRandomPort(int higher, int lower) {

		Random random = new Random();
		//numero entre 1 e higher
		int port = lower + random.nextInt((higher - 1)  - lower) + 1;
		//System.out.println("Random port: " + port);
		return port;

	}

	public static URL getGossipPeer() {

		//System.out.println("getGossipPeer CALLED");

		List<Node> nodes = new ArrayList<Node>();

		nodes.addAll(((ChordImpl)chord).getReferences().getSuccessors());
		nodes.add(((ChordImpl)chord).getReferences().getPredecessor());

		Random random = new Random();

		int index = random.nextInt(nodes.size());
		Node chosenNode;

		chosenNode = nodes.get(index);

		//System.out.println("PEER URL chosen: " + chosenNode.getNodeURL().toString());

		return chosenNode.getNodeURL();

	}

	public static void main(String[] args){

		if(args[0] == null && args[1] == null) {
			System.out.println("Invalid arguments!");
			System.exit(1);
		}

		Collection<URL> peersList;
		Collection<URL> peersToBeRemoved = new ArrayList<URL>();

		//username and mounting point
		String username = args[0];
		String folder = args[1];
		final String username2 = new String(username);
		
		//keys and contents
		MyKey keyRoot = new MyKey("/");
		String keyRootContent = "DIR\n";

		final MyKey usersKey = new MyKey("/admin/usersKey");
		
		final MyKey activeUsersKey = new MyKey("/admin/activeUserKey");
		final String activeUsersContent = username;
		//String usersKeyContent = "";

		//MyKey activeUsersKey = new MyKey("/admin/activeUsersKey");
		//MyKey duplicatedActiveUsersKey = new MyKey("/admin/duplicatedActiveUsersKey");

		//keys and contents

		//username and mounting point

		PropertiesLoader.loadPropertyFile();

		

		PeersFile peersFile = new PeersFile("https://dl.dropboxusercontent.com/u/23827391/peers", "peers");
		peersList = peersFile.getPeersList();

		chord = new ChordImpl();
		String protocol = URL.KNOWN_PROTOCOLS.get(URL.SOCKET_PROTOCOL);
		URL localURL = null;
		final int port = generateRandomPort(HIGHPORT, LOWPORT);
		try {
			//TODO: trocar localhost para ip da maquina
			localURL = new URL(protocol + "://localhost:" + port + "/");

		} catch(MalformedURLException e) {
			System.out.println(e.getMessage());
			System.exit(1);
		}

		//pode devolver NULL
		byte[] mac = getMacAdress();

		if(mac == null) {
			System.out.println("Could not create your ID because we couldnt get your MAC address");
			System.exit(1);
		}


		if(username.equals("superadmin")) {

			try {
				ByteArrayOutputStream macAndUser = new ByteArrayOutputStream();
				macAndUser.write(mac);
				macAndUser.write(username.getBytes());
				chord.create(localURL, new ID(getSHA1(macAndUser.toByteArray())));
				System.out.println("Create executed!");
				peersFile.prependPeerToPeerList(localURL);
				peersFile.save();
				while(true);
			} catch (ServiceException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}

		}

		if(peersList.isEmpty()) {
			System.out.println("PeersList empty!");
		}

		for(URL url : peersList) {
			try {

				ByteArrayOutputStream macAndUser = new ByteArrayOutputStream();
				macAndUser.write(mac);
				macAndUser.write(username.getBytes());

				chord.join(localURL, new ID(getSHA1(macAndUser.toByteArray())), url);

				chord.insert(usersKey, username);

				//socket = new DatagramSocket(chord.getURL().getPort());

				final Gossip gossip = new Gossip();		

				if(username.equals("admin")) {

					boolean firstTime = false;
					Set<Serializable> data = chord.retrieve(keyRoot);
					firstTime = data.isEmpty();

					//if(firstTime) {
					
					gossip.initActiveNodes(1,1);
					
					gossip.initAverageFiles(getNumberOfFiles(), 1);
					gossip.initAverageMb(getNumberOfFileMBytes(), 1);
					
					
					chord.insert(keyRoot, keyRootContent);
					//} else {
					//gossip.setValues(1, 1, 1, 1, 9, 1, (double)Integer.parseInt(args[2]), 1);//os dois ultimos deviam ser os tamanhos dos ficheiros	
					//}


				} else {
					
					gossip.initActiveNodes(1, 0);
					
					
					gossip.initAverageFiles(getNumberOfFiles(), 1);
					gossip.initAverageMb(getNumberOfFileMBytes(), 1);
					
				}

				Thread udpReceiverThread = new Thread(new Runnable(){	

					@Override
					public void run() {
						ServerSocket welcomeSocket = null;
						try {
							welcomeSocket = new ServerSocket(port + 1);
						} catch (IOException e1) {
							e1.printStackTrace();
						}
						while(true) {

							try {

								final Socket connectionSocket = welcomeSocket.accept();

								Thread clientAcceptThread = new Thread(new Runnable() {

									@Override
									public void run() {

										ObjectInputStream inFromClient = null;
										try {
											
											
											Message message;
											inFromClient = new ObjectInputStream(connectionSocket.getInputStream());

											//DataOutputStream outToClient = new DataOutputStream(connectionSocket.getOutputStream());
											message = (Message) inFromClient.readObject();
											gossip.processMessage(message);
											System.out.println("MESSAGE RECEIVED: " + message.toString());


											inFromClient = new ObjectInputStream(connectionSocket.getInputStream());
											message = (Message) inFromClient.readObject();
											gossip.processMessage(message);
											System.out.println("MESSAGE RECEIVED: " + message.toString());

											inFromClient = new ObjectInputStream(connectionSocket.getInputStream());
											message = (Message) inFromClient.readObject();
											gossip.processMessage(message);
											System.out.println("MESSAGE RECEIVED: " + message.toString());
											

										} catch (IOException e) {
											e.printStackTrace();
											System.out.println("CATCH SENDER 1");
										} catch (ClassNotFoundException e) {
											e.printStackTrace();
											System.out.println("CATCH SENDER 2");
										}
									}

								});

								clientAcceptThread.start();
											
							} catch (SocketException e) {
								//System.out.println("CATCH SENDER 3");
								//e.printStackTrace();
							} catch (UnknownHostException e) {
								//System.out.println("CATCH SENDER 4");
								//e.printStackTrace();
							} catch (IOException e) {
								//System.out.println("CATCH SENDER 5");
								//e.printStackTrace();
							} finally {
								//System.out.println("FINALLY");
								//socket.close();
							}
						}


					}
				});

				udpReceiverThread.start();

				Thread udpSender = new Thread(new Runnable(){

					@Override
					public void run(){ //tenho ip porta

						Socket clientSocket = null;
						
						int round = 0;
						
						while(true) {

							
							try {
								
								if(username2.equals("admin") && (round%20==0)){
									gossip.resetAll();
									gossip.newGossipRound();
									System.out.println("REINICIOU");
								}
								
								URL url = getGossipPeer();
								clientSocket = new Socket(url.getHost(), url.getPort() + 1);
								
								System.out.println("DEPOIS DO SOCKET NO RECEIVER");
								
								if(clientSocket.isConnected()) {
									System.out.println("CLIENT SOCKET IS CONNECTED");
								} else {
									System.out.println("CLIENT SOCKET IS NOT CONNECTED");
									continue;
								}
								
								List<Message> messages = new ArrayList<Message>();
								messages.add(gossip.getMessage(MessageType.Q1));
								messages.add(gossip.getMessage(MessageType.Q3));
								messages.add(gossip.getMessage(MessageType.Q4));	
								
								
								
									//Message msg  = gossip.getLogOutMessage(MessageType.Q3);
									
									//ObjectOutputStream outToServer = new ObjectOutputStream(clientSocket.getOutputStream());
									//outToServer.writeObject(msg);
						
								for(Message msg : messages){

									System.out.println("MESSAGE SENT: " + msg.toString());

									ObjectOutputStream outToServer = new ObjectOutputStream(clientSocket.getOutputStream());
									outToServer.writeObject(msg);

								}
								
								System.out.println("");
								System.out.println("");
								System.out.println("GOSSIP 1 AVERAGE: " + gossip.average(MessageType.Q1));
								System.out.println("GOSSIP 3 AVERAGE: " + gossip.average(MessageType.Q3));
								System.out.println("GOSSIP 4 AVERAGE: " + gossip.average(MessageType.Q4));
								
								System.out.println("");
								System.out.println("");

								
								
								clientSocket.close();

							} catch (Exception e) {
								System.out.println("CATCH DO RECEIVER");
								//e.printStackTrace();	
							} finally {
								//System.out.println("FINALLY");
							}
							
							try {
								Thread.sleep(1000);
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							round++;

						}

					}

				});

				udpSender.start();

				/////////////////
				Thread activeUsersUpdater = new Thread(new Runnable(){

					@Override
					public void run(){ 
						
						int round = 1;
						
						while(true) {

							
							try {
								
								if(username2.equals("admin") && (round%20==0)){

									System.out.println("ADMIN REINICIA COUNT E VISUALIZA SETS");
									
									Set<Serializable> activeUsersSet = chord.retrieve(activeUsersKey);
									
									for(Serializable ser : activeUsersSet) {
										
										chord.remove(activeUsersKey, ser);
										
										System.err.println("ACTIVE USERS: " + ser.toString());
									}
									
									System.err.println("COUNT ACTIVE USERS: " + activeUsersSet.size());
									
									chord.insert(activeUsersKey, username2);
									
									//visualização do set de users totais na rede
									
									Set<Serializable> usersSet = chord.retrieve(usersKey);
									
									for(Serializable ser : usersSet) {
										
										System.err.println("USERS: " + ser.toString());
										
									}
									
									System.err.println("USERS COUNT: " + usersSet.size());
									
									
								} else {
									
									chord.insert(activeUsersKey, activeUsersContent);
									
								}
								
								Thread.sleep(1000);
								
								
								round++; 

							} catch (Exception e) {
								System.out.println("CATCH DO RECEIVER");
								//e.printStackTrace();	
							} finally {
								//System.out.println("FINALLY");
							}
							
							try {
								Thread.sleep(1000);
							} catch (InterruptedException e) {
								e.printStackTrace();
							}

						}

					}

				});

				activeUsersUpdater.start();				
				
				
				
				
				
				/////////////////////////////7
				
				break; //sai da lista de peers se conseguir fazer join

			} catch (ServiceException e) {
				peersToBeRemoved.add(url);
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}

		}


		peersFile.removePeersInPeersList(peersToBeRemoved);
		int peersInList = peersList.size();
		//System.out.println("PEERS IN THE LIST OF PEERS" + peersInList);

		peersFile.prependPeerToPeerList(localURL);
		peersFile.save();

		//verfica se tem que ir buscar mais ips ao "DNS"
		if((2/3) * N > peersInList) {


			//chama funcao que faz download do ficheiro peers global
		}

		Fs fs = Fs.initializeFuse(chord, folder, false);

	}


	public static int getNumberOfFiles() {

		int num = 0;
		for (Map.Entry<ID, Set<Entry>> entry : ((ChordImpl) chord).getEntries().getEntries().entrySet()) {
			for (Entry setEntry : entry.getValue()) {
				num++;
			}
		}		
		System.out.println("NUMBER OF FILES: " + num);
		return num;
	}

	//TODO: modificar o codigo getSize da ENTRY e meter isto tudo a devolver um tuplo para optimizar
	public static double getNumberOfFileMBytes() {

		double bytes = 0;
		for (Map.Entry<ID, Set<Entry>> entry : ((ChordImpl) chord).getEntries().getEntries().entrySet()) {
				
			for (Entry setEntry : entry.getValue()) {
				if(setEntry.getValue() instanceof String) {
					bytes += ((String) setEntry.getValue()).length();					
				} else {
					bytes += ((byte[]) setEntry.getValue()).length;
				}
				
				
			}

		}	

		System.out.println("NUMBER OF BYTES: " + bytes);

		return bytes/(1024*1024);

	}

}

