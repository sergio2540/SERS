package fuse;


import gossip.core.Gossip;
import gossip.message.Message;
import gossip.message.MessageType;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import chord.HbaseManager;
import chord.Metadata;
import chord.MetadataType;
import chord.MyKey;
import chord.PeersFile;
import de.uniba.wiai.lspi.chord.com.Entry;
import de.uniba.wiai.lspi.chord.com.Node;
import de.uniba.wiai.lspi.chord.data.ID;
import de.uniba.wiai.lspi.chord.data.URL;
import de.uniba.wiai.lspi.chord.service.Chord;
import de.uniba.wiai.lspi.chord.service.ServiceException;
import de.uniba.wiai.lspi.chord.service.impl.ChordImpl;
import net.fusejna.DirectoryFiller;
import net.fusejna.ErrorCodes;
import net.fusejna.FuseException;
import net.fusejna.StructFuseFileInfo.FileInfoWrapper;
import net.fusejna.StructStat.StatWrapper;
import net.fusejna.types.TypeMode.ModeWrapper;
import net.fusejna.types.TypeMode.NodeType;
import net.fusejna.util.FuseFilesystemAdapterAssumeImplemented;
import chord.Client;



public class Fs extends FuseFilesystemAdapterAssumeImplemented {

	private static Fs fs;
	private Chord chord;
	private URL userURL;
	private HbaseManager manager;
	private PeersFile file;

	private final int BLOCKSIZE = 4097;

	public URL getUserURL() {
		return userURL;
	}

	public void setUserURL(URL userURL) {
		this.userURL = userURL;
	}

	public HbaseManager getManager() {
		return manager;
	}

	public void setManager(HbaseManager manager) {
		this.manager = manager;
	}

	public Fs() {}

	private Fs(Chord chord, String path, URL userURL, HbaseManager manager, PeersFile file) {
		this.chord = chord;
		this.userURL = userURL;
		this.manager = manager;
		this.file = file;

	}   

	public static Fs initializeFuse(Chord chord, String path, boolean debug, URL userURL, HbaseManager manager, PeersFile file) {

		if(fs == null) {
			fs = new Fs(chord, path, userURL, manager, file);
		}

		try {

			fs.log(debug).mount(path);

		} catch (FuseException e) {
			e.printStackTrace();
		}

		return fs;

	}


	@Override
	public int getattr(final String path, final StatWrapper stat) {

		String metaInfo[] = null;
		String fileOrDir = "";
		int size = 0;

		Set<Serializable> dataSet = null;
		try {
			dataSet = chord.retrieve(new MyKey(path));
		} catch (ServiceException e) {
			e.printStackTrace();
		}

		for(Serializable data : dataSet) {
			metaInfo = data.toString().split("\n");
			fileOrDir = metaInfo[0];
		}

		if (fileOrDir.equals("DIR")) {
			stat.setMode(NodeType.DIRECTORY);
			return 0;
		}

		if(fileOrDir.equals("FILE")) {
			size = Integer.parseInt(metaInfo[1]);
			stat.setMode(NodeType.FILE).size(size);
			return 0;
		}

		return -ErrorCodes.ENOENT();

	}

	@Override 
	public int open(final String path, final FileInfoWrapper info) {
		return 0;
	}

	@Override
	public int read(final String path, final ByteBuffer buffer, final long size, final long offset, final FileInfoWrapper info)
	{

		int beginOffSet = (int) offset/(BLOCKSIZE-1);

		int endOffSet = (int ) ((size + offset) / (BLOCKSIZE));

		String metaInfo[] = null;
		
		ByteArrayOutputStream bos = new ByteArrayOutputStream();

		try {

			Collection<Serializable> list = new ArrayList<Serializable>();

			Set<Serializable> dataSet = null;
			dataSet = chord.retrieve(new MyKey(path));

			if(dataSet.isEmpty()) {
				return -ErrorCodes.ENOENT();
			} 

			for(Serializable data : dataSet) {
				metaInfo = data.toString().split("\n");
			}

			for(int i = beginOffSet + 2; i <= endOffSet+2; i++) {
				list.addAll(chord.retrieve(new MyKey(metaInfo[i])));
			}

			for(Serializable data : list) {
				try {
					bos.write((byte[]) data);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

		} catch (ServiceException e) {
			e.printStackTrace();
		}

		byte [] buf = bos.toByteArray();

		buffer.put(buf);

		return (int)size;

	}

	@Override
	public int readdir(final String path, final DirectoryFiller filler)
	{

		Set<Serializable> dataSet = null;
		String metaInfo[] = null;
		String fileOrDir = ""; 

		try {
			dataSet = chord.retrieve(new MyKey(path));
		} catch (ServiceException e) {
			e.printStackTrace();
		}

		for(Serializable data : dataSet) {
			metaInfo = data.toString().split("\n");
			fileOrDir = metaInfo[0];
		}

		if(!fileOrDir.equals("DIR")) {
			return -ErrorCodes.ENOTDIR();
		}

		boolean times = false;
		int n = 0;

		for(int i = 1; i < metaInfo.length; i++) {
			times = filler.add(metaInfo[i]);
			if(times) {
				n++;
			}
		}

		return 0;

	}

	@Override
	public int write(final String path, final ByteBuffer buf, final long bufSize, final long writeOffset, final FileInfoWrapper wrapper) {

		
		int beginOffSet = (int) writeOffset/(BLOCKSIZE-1);

		int endOffSet = (int ) ((bufSize + writeOffset) / (BLOCKSIZE));

		MyKey fileKey = new MyKey(path);

		Set<Serializable> fileSet = null;

		try {

			fileSet = chord.retrieve(fileKey);

			Metadata fileMetadata = null;

			for(Serializable metadata : fileSet) {
				chord.remove(fileKey, metadata);
				fileMetadata = Metadata.createMetadata(metadata.toString());
			}

			List<String> blocks = (List<String>) fileMetadata.getBlocksPaths();

			int bufIndex = 0;

			ByteArrayOutputStream newBlock = new ByteArrayOutputStream(BLOCKSIZE);

			for(int blockNo = beginOffSet; blockNo <= endOffSet; blockNo++){

				newBlock.reset();
				
				int oldBlockLength = 0;

				if(blockNo < blocks.size()) {

					String block = blocks.get(blockNo);

					Set<Serializable> data = chord.retrieve(new MyKey(block));
					if(!data.isEmpty()) {

						for(Serializable ser : data) {
							oldBlockLength += ((byte[])ser).length; 
							chord.remove(new MyKey(block), ser);
						}

					}

				}

				for (int index2 = bufIndex; index2 < bufSize;index2++) {

					if(newBlock.size() == BLOCKSIZE){			
						bufIndex = index2;
						break;
					}

					newBlock.write(buf.get(index2));

				}

				byte[] buffer = newBlock.toByteArray();

				String shaBuffer = new String(Client.bytesToHex(Client.getSHA1(buffer)));

				fileMetadata.updateBlock(blockNo, shaBuffer);

				int blockDiff = newBlock.size() - oldBlockLength;

				fileMetadata.inc(blockDiff);

				MyKey key = new MyKey(shaBuffer);
				chord.insert(key, buffer);

			}


			chord.insert(fileKey, fileMetadata.getMetadata());

		} catch(Exception e) {
			System.out.println(e.getMessage());
		}

		return (int) bufSize;

	}	


	@Override
	public int create(final String path, final ModeWrapper mode, final FileInfoWrapper info) {

		mode.setMode(NodeType.FILE);

		Set<Serializable> dataSet = null;

		Path pathObject = FileSystems.getDefault().getPath(path);

		try {

			dataSet = chord.retrieve(new MyKey(path));

			if(dataSet.isEmpty()) {

				MyKey key = new MyKey(pathObject.getParent().toString());

				Set<Serializable> set = chord.retrieve(key);
				Metadata newMetadata = null;

				for(Serializable oldMetadata : set) {
					chord.remove(key, oldMetadata);
					newMetadata = Metadata.createMetadata(oldMetadata.toString());
				}

				newMetadata.addFile(pathObject.getFileName().toString());

				chord.insert(key, newMetadata.getMetadata());				

				key = new MyKey(path);

				Metadata content = Metadata.createMetadata(MetadataType.FILE.name() + "\n");
				chord.insert(key, content.getMetadata());


			} else {

				return -ErrorCodes.EEXIST();

			}

		} catch (ServiceException e) {
			e.printStackTrace();
		}

		return 0;

	}

	@Override
	public int rename(final String path, final String newName) {

		Set<Serializable> dataSet1 = null;
		Metadata newMetadata = null;

		Path pathObject1 = FileSystems.getDefault().getPath(path);
		Path pathObject2 = FileSystems.getDefault().getPath(newName);

		MyKey oldKey = new MyKey(pathObject1.toString());
		MyKey oldParentKey = new MyKey(pathObject1.getParent().toString());
		MyKey newKey = new MyKey(pathObject2.toString());

		try {

			dataSet1 = chord.retrieve(new MyKey(pathObject1.toString()));

			for(Serializable oldMetadata : dataSet1) {
				chord.remove(oldKey, oldMetadata);
				newMetadata = Metadata.createMetadata(oldMetadata.toString());
			}

			chord.insert(newKey, newMetadata.getMetadata());

			dataSet1 = chord.retrieve(new MyKey(pathObject1.getParent().toString()));
			oldParentKey = new MyKey(pathObject1.getParent().toString());

			for(Serializable oldMetadata : dataSet1) {
				chord.remove(oldParentKey, oldMetadata);
				newMetadata = Metadata.createMetadata(oldMetadata.toString());
			}

			newMetadata.removeFile(pathObject1.toString());

			chord.insert(oldParentKey, newMetadata.getMetadata());

			dataSet1 = chord.retrieve(new MyKey(pathObject2.getParent().toString()));
			oldKey = new MyKey(pathObject2.getParent().toString());

			for(Serializable oldMetadata : dataSet1) {
				chord.remove(oldKey, oldMetadata);
				newMetadata = Metadata.createMetadata(oldMetadata.toString());
			}

			chord.insert(newKey, newMetadata.getMetadata());

			MyKey key = new MyKey(pathObject2.getParent().toString());

			Set<Serializable> set = chord.retrieve(key);

			for(Serializable oldMetadata : set) {
				chord.remove(key, oldMetadata);
				newMetadata = Metadata.createMetadata(oldMetadata.toString());
			}

			newMetadata.addFile(pathObject2.toString());

			chord.insert(key, newMetadata.getMetadata());


		} catch (ServiceException e) {
			e.printStackTrace();
		}

		return 0;
	}

	@Override
	public int unlink(final String path) {

		Set<Serializable> dataSet = null;

		Path pathObject = FileSystems.getDefault().getPath(path);

		try {

			dataSet = chord.retrieve(new MyKey(path));

			if(!dataSet.isEmpty()) {

				MyKey key = new MyKey(pathObject.getParent().toString());

				Set<Serializable> set = chord.retrieve(key);
				Metadata newMetadata = null;

				for(Serializable oldMetadata : set) {
					chord.remove(key, oldMetadata);
					newMetadata = Metadata.createMetadata(oldMetadata.toString());
				}

				newMetadata.removeFile(pathObject.getFileName().toString());
				chord.insert(key, newMetadata.getMetadata());				

				key = new MyKey(path);

				Metadata content = Metadata.createMetadata(MetadataType.FILE.name() + "\n");

				for(String block : content.getBlocksPaths()) {
					for(Serializable sr : chord.retrieve(new MyKey(block))) { 
						chord.remove(new MyKey(block), sr);
					}
				}

				for(Serializable sr : dataSet) {
					chord.remove(key, sr);
				}

			} else {
				return -ErrorCodes.ENOENT();
			}

		} catch(Exception e) {
			System.out.println(e.getMessage());
		}

		return 0;

	}

	@Override
	public int mkdir(final String path, final ModeWrapper mode)
	{

		mode.setMode(NodeType.DIRECTORY, true, true, true, true, true, true, true, true, true);
		Set<Serializable> dataSet = null;

		Path pathObject = FileSystems.getDefault().getPath(path);

		try {

			dataSet = chord.retrieve(new MyKey(path));

			if(dataSet.isEmpty()) {

				MyKey key = new MyKey(pathObject.getParent().toString());

				Set<Serializable> set = chord.retrieve(key);
				Metadata newMetadata = null;

				for(Serializable oldMetadata : set) {
					chord.remove(key, oldMetadata);
					newMetadata = Metadata.createMetadata(oldMetadata.toString());
				}

				newMetadata.addDir(pathObject.getFileName().toString());

				chord.insert(key, newMetadata.getMetadata());				

				key = new MyKey(path);

				Metadata content = Metadata.createMetadata(MetadataType.DIR.name());
				chord.insert(key, content.getMetadata());


			} else {

				return -ErrorCodes.EEXIST();

			}

		} catch (ServiceException e) {
			e.printStackTrace();
		}

		int bytes = 0;
		for (Map.Entry<ID, Set<Entry>> entry : ((ChordImpl) chord).getEntries().getEntries().entrySet()) {

			for (Entry setEntry : entry.getValue()) {
				bytes += setEntry.getValue().toString().getBytes().length;
			}

		}

		URL url = ((ChordImpl) chord).getReferences().getPredecessor().getNodeURL();

		return 0;

	}

	@Override
	public int rmdir(final String path)
	{

		Set<Serializable> dataSet = null;

		Path pathObject = FileSystems.getDefault().getPath(path);

		try {

			Metadata newMetadata = null;

			dataSet = chord.retrieve(new MyKey(path));

			if(!dataSet.isEmpty()) {

				int i = 0;
				for (Serializable serial : dataSet) {

					i++;

					newMetadata = Metadata.createMetadata(serial.toString());
					if(newMetadata.getFiles().size() != 0) {
						return -ErrorCodes.ENOTEMPTY();
					}
				}

				for(Serializable oldMetadata : dataSet) {
					chord.remove(new MyKey(path), oldMetadata);	
				}

				MyKey key = new MyKey(pathObject.getParent().toString());

				Set<Serializable> set = chord.retrieve(key);

				for(Serializable tempSet : set) {
					chord.remove(key, tempSet);
					newMetadata = Metadata.createMetadata(tempSet.toString());
				}
				newMetadata.rmDir(pathObject.getFileName().toString());

				chord.insert(key, newMetadata.getMetadata());				

			} else {

				return -ErrorCodes.ENOENT();

			}

		} catch (ServiceException e) {
			e.printStackTrace();
		}

		return 0;

	}

	@Override
	public int access(final String path, final int access) {
		return 0;
	}

	@Override 
	public void beforeUnmount(final File mountpoint) {
		Gossip gossip = Client.getGossip();
		

		
		while(true) {
			
			URL url = Client.getGossipPeer();
			Socket clientSocket;
			try {
				
				clientSocket = new Socket(url.getHost(), url.getPort() + 1);
	
			if(clientSocket.isConnected()) {
			} else {
				continue;
			}
	
			List<Message> messages = new ArrayList<Message>();
			messages.add(gossip.getLogOutMessage(MessageType.Q1));
			messages.add(gossip.getLogOutMessage(MessageType.Q3));
			messages.add(gossip.getLogOutMessage(MessageType.Q4));	
	
			for(Message msg : messages){
				
				ObjectOutputStream outToServer = new ObjectOutputStream(clientSocket.getOutputStream());
				outToServer.writeObject(msg);
				
			}
			
				break;
		
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
		}
			
		file.removePeerInPeersList(userURL);
		file.save();
	}

	@Override
	public void afterUnmount(final File mountPoint) {
		mountPoint.delete();
	}

}
