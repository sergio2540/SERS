package chord;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
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
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import org.jboss.netty.channel.local.LocalAddress;
import org.jruby.compiler.ir.operands.Hash;

import de.uniba.wiai.lspi.chord.com.Entry;
import de.uniba.wiai.lspi.chord.com.Node;
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
	static int DNSMAX = 15;

	private static Chord chord;
	
	static URL localURL = null;

	private static HbaseManager manager;
	public static byte[] getSHA1(byte[] username) {

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
		
		byte[] macInBytes = null;
		
		try {
			
			String[] command = {"/bin/sh", "-c", "ip link show eth0 | awk '/ether/ {print $2}'"};
		    Process pid = Runtime.getRuntime().exec(command);
		    
		    BufferedReader in = new BufferedReader(new InputStreamReader(pid.getInputStream()));
			macInBytes =  in.readLine().getBytes();
			
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	
		return macInBytes;
	    
	}

//	private static byte[] getMacAdress() {
//
//		InetAddress ip;
//
//		StringBuilder sb = null;
//
//		try {
//
//			ip = InetAddress.getLocalHost();
//			//System.out.println("Current IP address : " + ip.getHostAddress());
//			Enumeration<NetworkInterface> nets = NetworkInterface.getNetworkInterfaces();
//			NetworkInterface netint;
//			List<InterfaceAddress> interfaces = new ArrayList<InterfaceAddress>();
//
//			while(nets.hasMoreElements()) {
//				netint = nets.nextElement();
//				interfaces.addAll(netint.getInterfaceAddresses());
//			}
//
//			//for(InterfaceAddress intAddr : interfaces){
//			//	System.out.println("| " + intAddr.toString() + " |");
//			//}
//
//			ip = InetAddress.getByAddress(interfaces.get(1).getAddress().getAddress());
//			NetworkInterface network = NetworkInterface.getByInetAddress(ip);
//
//			if(network == null) {
//				System.out.println("NULL");
//				return "".getBytes();
//			}
//
//			byte[] mac = network.getHardwareAddress();
//
//			sb = new StringBuilder();
//
//			for (int i = 0; i < mac.length; i++) {
//				sb.append(String.format("%02X%s", mac[i], (i < mac.length - 1) ? "-" : ""));		
//			}
//
//			//System.out.println(sb.toString());
//
//		} catch (UnknownHostException | SocketException e) {
//			e.printStackTrace();
//		}
//
//		return sb.toString().getBytes();
//
//	}

	private static int generateRandomPort(int higher, int lower) {

		Random random = new Random();
		//numero entre 1 e higher
		int port = lower + random.nextInt((higher - 1)  - lower) + 1;
		System.out.println("Random port: " + port);
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

		
		manager = new HbaseManager();
		manager.prepareDB();
		Collection<URL> peersList = null;
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

		PropertiesLoader.loadPropertyFile();

		//PeersFile peersFile = new PeersFile("https://dl.dropboxusercontent.com/u/23827391/peers", "peers");
		PeersFile peersFile = null;

		chord = new ChordImpl();
		String protocol = URL.KNOWN_PROTOCOLS.get(URL.SOCKET_PROTOCOL);
		
		final int port = generateRandomPort(HIGHPORT, LOWPORT);
		try {
			//TODO: trocar localhost para ip da maquina
			
			InetAddress myself = null;
			try {
				myself = InetAddress.getLocalHost();
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
					System.out.println(myself.getHostAddress());
			
			localURL = new URL(protocol + "://" + "localhost" + ":" + port + "/");

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
				manager.prepareDB();
				manager.insert(protocol + "://" + localURL.getHost() + ":" + localURL.getPort() + "/");
				manager.cleanUp();
				peersFile = new PeersFile("peers");
				//peersList = peersFile.getPeersList();
				//peersFile.prependPeerToPeerList(localURL);
				//peersFile.save();
				while(true);
			} catch (ServiceException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}

		}
		
		peersFile = new PeersFile("peers");
		peersList = peersFile.getPeersList();

		if(peersList.isEmpty()) {
			System.out.println("PeersList empty!");
		}

		for(URL url : peersList) {
			try {

				System.out.println(url.getHost() + url.getPort());
				
				ByteArrayOutputStream macAndUser = new ByteArrayOutputStream();
				macAndUser.write(mac);
				macAndUser.write(username.getBytes());

				chord.join(localURL, new ID(getSHA1(macAndUser.toByteArray())), url);

				chord.insert(usersKey, username);

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
					manager.prepareDB();
					manager.insert(protocol + "://" +localURL.getHost() + ":" + localURL.getPort() + "/");
					manager.cleanUp();
					
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

				Thread activeUsersUpdater = new Thread(new Runnable(){

					@Override
					public void run(){ 
						
						int round = 1;
						
						while(true) {

							
							try {
								
								if(username2.equals("admin") && (round%20==0)){

									System.out.println("ADMIN REINICIA COUNT E VISUALIZA SETS");
									
									Set<Serializable> activeUsersSet = chord.retrieve(activeUsersKey);
									Set<String> activeUsers = new HashSet<String>();
									int count = 0;
									
									String[] parsedSet;
									String bestPeers = "";
									
									manager.prepareDB();
									manager.deleteAll();
									
									for(Serializable ser : activeUsersSet) {
										
										chord.remove(activeUsersKey, ser);
										
										
										parsedSet = ser.toString().split(" ");
										activeUsers.add(parsedSet[0]);
										
										if(count < DNSMAX) {
											manager.insert(parsedSet[1]);
										}
										
										count++;
										
										//System.err.println("ACTIVE USERS: " + ser.toString());
									}
									
									try {
										manager.cleanUp();
									} catch (IOException e) {
										e.printStackTrace();
									}
									
									System.err.println("COUNT ACTIVE USERS: " + activeUsers.size());
									
									chord.insert(activeUsersKey, username2 + " " + localURL.getProtocol() + "://" + 
											localURL.getHost() + ":" + localURL.getPort() + "/");
									
									//visualização do set de users totais na rede
									
									Set<Serializable> usersSet = chord.retrieve(usersKey);
									
									for(Serializable ser : usersSet) {
										
										//System.err.println("USERS: " + ser.toString());
										
									}
									
									System.err.println("COUNT USERS: " + usersSet.size());
									
									
								} else {
									
									chord.insert(activeUsersKey, activeUsersContent + " " + localURL.getProtocol() + "://" + 
									localURL.getHost() + ":" + localURL.getPort() + "/");
									
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

				break; //sai da lista de peers se conseguir fazer join

			} catch (ServiceException e) {
				e.printStackTrace();
				peersToBeRemoved.add(url);
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println(e.getMessage());
			}

		}

		peersFile.removePeersInPeersList(peersToBeRemoved);
		int peersInList = peersList.size();
		//System.out.println("PEERS IN THE LIST OF PEERS" + peersInList);

		peersFile.prependPeerToPeerList(localURL);
		peersFile.save();

		//verfica se tem que ir buscar mais ips ao "DNS"
//		if(peersInList < 2/3 * N) {
//			System.out.println("peersInList < 2");
//			peersFile.getPeersList().clear();//???????????????????????????????????????????????????
//			peersFile.save();
//			peersFile.getPeersFromDB();
//
//			//chama funcao que faz download do ficheiro peers global
//		}
		
		Fs fs = Fs.initializeFuse(chord, folder, false, localURL, manager, peersFile);

	}


	public static int getNumberOfFiles() {

		int files = 0;
		
		for (Map.Entry<ID, Set<Entry>> entry : ((ChordImpl) chord).getEntries().getEntries().entrySet()) {
			for (Entry setEntry : entry.getValue()) {
				
				
				Metadata metadata = Metadata.createMetadata(setEntry.getValue().toString());
				if(metadata != null) {
					
					if(metadata.getMetadataType().ordinal() == MetadataType.FILE.ordinal()) { 
						files++;
					}
					
				} else {
					continue;
				}

			}
		}		
		System.out.println("NUMBER OF FILES: " + files);
		return files;
	}

	//TODO: modificar o codigo getSize da ENTRY e meter isto tudo a devolver um tuplo para optimizar
	public static double getNumberOfFileMBytes() {

		double bytes = 0;
		
		for (Map.Entry<ID, Set<Entry>> entry : ((ChordImpl) chord).getEntries().getEntries().entrySet()) {
				
			for (Entry setEntry : entry.getValue()) {
					Metadata metadata = Metadata.createMetadata(setEntry.getValue().toString());
					if(metadata != null) {
						bytes += (double) metadata.getSize();
						
					} else {
						continue;
					}
				
			}

		}	

		
		System.out.println("NUMBER OF BYTES: " + bytes);

		return bytes/(1024*1024);

	}

}

