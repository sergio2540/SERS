package fuse;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import chord.Metadata;
import chord.MetadataType;
import chord.MyKey;
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

	//private static String content = "";

	private final int BLOCKSIZE = 4097;
	//private byte buffer[] = new byte[BLOCKSIZE];
	//private ByteBuffer byteBuffer = ByteBuffer.allocate(BLOCKSIZE);

	public Fs() {}

	private Fs(Chord chord, String path) {
		this.chord = chord;
	
	}   

	public static Fs initializeFuse(Chord chord, String path, boolean debug) {

		System.out.println("function initializeFuse (Fs.java)");

		if(fs == null) {
			fs = new Fs(chord, path);
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

		//System.out.println("function getattr (Fs.java) PATH: " + path);

		String metaInfo[] = null;
		String fileOrDir = "";
		int size = 0;

		//REMOVER
		//File file = new File(path);

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

		if (fileOrDir.equals("DIR")) { // Root directory
			//System.out.println("FILE IS DIRECTORY!");
			stat.setMode(NodeType.DIRECTORY);
			return 0;
		}

		if(fileOrDir.equals("FILE")) {
			size = Integer.parseInt(metaInfo[1]);
			//System.out.println("FILE IS FILE!");
			stat.setMode(NodeType.FILE).size(size);
			return 0;
		}

		//System.out.println("not a file or dir");

		return -ErrorCodes.ENOENT();

	}

	@Override 
	public int open(final String path, final FileInfoWrapper info) {
		System.out.println("open - Fs.java");
		return 0;

	}

	@Override
	public int read(final String path, final ByteBuffer buffer, final long size, final long offset, final FileInfoWrapper info)
	{

		//String content = "";
	
		System.out.println("function read (Fs.java)");
		System.out.println("SIZE: " + size);
		System.out.println("OFFSET: " + offset);
		
		//2100/1024 = indice 2 bloco
		int beginOffSet = (int) offset/(BLOCKSIZE-1);

		// (1048 + 2100) /1024 = indice 3 bloco
		int endOffSet = (int ) ((size + offset) / (BLOCKSIZE));
		
		//System.out.println("----read start---");
		//System.out.println("path: " + path);
		//System.out.println("----read---");
		//System.out.println("<read>");

		//String s = "";
		String metaInfo[] = null;
		//String fileOrDir = "";
		ByteArrayOutputStream bos = new ByteArrayOutputStream();

		try {

			Collection<Serializable> list = new ArrayList<Serializable>();

			Set<Serializable> dataSet = null;
			dataSet = chord.retrieve(new MyKey(path));

			if(dataSet.isEmpty()) {

				System.out.println("data empty");
				return -ErrorCodes.ENOENT();

			} 
			
			
			

				for(Serializable data : dataSet) {
					metaInfo = data.toString().split("\n");
					//fileOrDir = metaInfo[0];
				}

	

				//if(fileOrDir.equals("DIR")) {
				//	System.out.println("function read - Fs.java");
				//	return -ErrorCodes.EISDIR();
				//} else if(fileOrDir.equals("FILE")) {
				
			
				
					System.out.println("INFO SIZE DO FILE: " + metaInfo[1]);
					
					for(int i = beginOffSet + 2; i <= endOffSet+2; i++) {
						//System.out.println("Bloco n" + (i-2));
						list.addAll(chord.retrieve(new MyKey(metaInfo[i])));

					}

					for(Serializable data : list) {
						//System.out.println("READ-------------" + data.toString());
						try {
							bos.write((byte[]) data);
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						//content += data.toString();
					}

					//TODO: mudar o content
					//content = content.trim();

					//System.out.println("CONTENT ON READ: " + content);

				//}

			
		} catch (ServiceException e) {
			e.printStackTrace();
		}

		//System.out.println("entrou");
		
		//ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
		//String content = new String(bos.toByteArray());
		// Compute substring that we are being asked to read
		//String s = content.substring((int) offset, (int) Math.max(offset, Math.min(content.length() - offset, offset + size)));
		
		byte [] buf = bos.toByteArray();
		
		//buffer.limit((int)bos.toByteArray().length);
		System.out.println("L: " + buf.length);
		
		
		//ByteArrayInputStream bis = new ByteArrayInputStream(buf);
		//byte[] read = new byte[(int)bos.toByteArray().length];
		//bis.read(read,(int)offset, (int)size);
		//System.out.println("S:" + read.length);
		
		//byte [] read = Arrays.copyOfRange(buf, (int)0, (int)(size));
		buffer.put(buf);
		
		
		//System.out.println("saiu bytes: " + read[0]);

		return (int)size;

	}

	@Override
	public int readdir(final String path, final DirectoryFiller filler)
	{

		System.out.println("function readdir (Fs.java)");

		Set<Serializable> dataSet = null;
		String metaInfo[] = null;
		String fileOrDir = ""; 

		try {
			dataSet = chord.retrieve(new MyKey(path));
		} catch (ServiceException e) {
			e.printStackTrace();
		}

		for(Serializable data : dataSet) {
			System.out.println("Data: " + data);
			metaInfo = data.toString().split("\n");
			fileOrDir = metaInfo[0];
		}

		if(!fileOrDir.equals("DIR")) {
			System.out.println("!fileOrDir.equals(DIR) - Fs.java");
			return -ErrorCodes.ENOTDIR();
		}

		boolean times = false;
		int n = 0;

		for(int i = 1; i < metaInfo.length; i++) {
			System.out.println(metaInfo[i]);
			System.out.println("metaInfo: " + metaInfo[i]);
			times = filler.add(metaInfo[i]);
			if(times) {
				n++;
			}
		}

		System.out.println("N: "+ n);

		System.out.println(filler.toString());

		return 0;

	}

	@Override
	public int write(final String path, final ByteBuffer buf, final long bufSize, final long writeOffset, final FileInfoWrapper wrapper) {
		
		//bufsize = 1048
		//writeoffset= 2100
		//System.out.println("||||||||||||||||||||||||||||||||||||||||||||||||");

		//System.out.println("bufSize: " + bufSize + "writeOffSet: " + writeOffset);

		//2100/1024 = indice 2 bloco
		int beginOffSet = (int) writeOffset/(BLOCKSIZE-1);

		// (1048 + 2100) /1024 = indice 3 bloco
		int endOffSet = (int ) ((bufSize + writeOffset) / (BLOCKSIZE));


		//2100%1024 = 52 bytes correctos
		//int cursorStartByte = (int) writeOffset % BLOCKSIZE;

		//(2100 + 1048) % 1024 = 76 
		//(2100 + 1048) = 3148 escrever ate 3148
		//int cursorEndByte = (int) (bufSize + writeOffset);

		//path = hello.txt
		MyKey fileKey = new MyKey(path);

		Set<Serializable> fileSet = null;
		
		try {

			fileSet = chord.retrieve(fileKey);

			Metadata fileMetadata = null;

			//Path pathObject = FileSystems.getDefault().getPath(path);

			for(Serializable metadata : fileSet) {
				chord.remove(fileKey, metadata);
				fileMetadata = Metadata.createMetadata(metadata.toString());
				//System.out.println("Inicio metadata do ficheiro: \n" + fileMetadata.getMetadata());
			}

			//ArrayList<String> blocks = (ArrayList<String>) fileMetadata.getBlocksPaths();//-------------------sapateiro
			
			List<String> blocks = (List<String>) fileMetadata.getBlocksPaths();
			//ArrayList<String> copy = new ArrayList<String>();
			
			//copia
			//for(String temp : blocks){
				//copy.add(temp);
			//}
			
			int bufIndex = 0;
			
			ByteArrayOutputStream newBlock = new ByteArrayOutputStream(BLOCKSIZE);
			//ByteArrayOutputStream oldBlock = new ByteArrayOutputStream(BLOCKSIZE);
			
		
			
			//2 .. 3
			for(int blockNo = beginOffSet; blockNo <= endOffSet; blockNo++){
				
				//Reinicia buffers
				newBlock.reset();
				//oldBlock.reset();
				int oldBlockLength = 0;
				
				//System.out.println("BLOCK: " + blockNo);
				
				//Arrays.fill(buffer, (byte) 0); //LIMPAR BYTEBUFFER????

				//String content = "";
				
				//byte oldBuffer[] = new byte[BLOCKSIZE];

				if(blockNo < blocks.size()) {
					
					//System.out.println("ja existe o bloco");
					
					String block = blocks.get(blockNo);
					
					Set<Serializable> data = chord.retrieve(new MyKey(block));
					if(!data.isEmpty()) {
						
						//System.out.println("data is not empty");
						
						//System.out.println("copia o antigo");
						
					
						
						for(Serializable ser : data) {
							//System.out.println("---------->" + new String((byte[])ser));
							//oldBlock.write((byte[])ser);
							oldBlockLength += ((byte[])ser).length; 
							chord.remove(new MyKey(block), ser);
					    }
						
						
						//byte[] temp = content.getBytes();
						//oldBlock.write(temp);
						//newBlock.write(temp);


						//oldBuffer = content.trim().getBytes();

						//System.out.println("oldBuffer Size: " + oldBlock.size());

						//for(int index = 0 ;index < cursorStartByte; index++){
						//	buffer[index] = oldBuffer[index];
							//byteBuffer.put(index,oldBuffer[index]);//e se nao existir data 0 0 0 0 data
						//}
						
						
						//for(Serializable ser : data) {
							//System.out.println("-------------------------removeu data anterior" + block);
							//chord.remove(new MyKey(block), ser);
						//}

					}

				}

				//System.out.println("cursor byte: " + cursorStartByte);
				//System.out.println("bufIndex: " + bufIndex);
				//System.out.println("bufsize: " + bufSize);

				for (int index2 = bufIndex; index2 < bufSize;index2++) {

					//System.out.println("cursor byte: " + cursorStartByte);
					//System.out.println("bufsize: " + bufSize);

					if(newBlock.size() == BLOCKSIZE){			
						//System.out.println("index == BLOCKSIZE");
						//newBlock.reset();
						//cursorStartByte = 0;
						bufIndex = index2;
						break;
					}

					newBlock.write(buf.get(index2));
					
					//byteBuffer.put(index, buf.get(index2));
				}

				/*
				Metadata newMetadata = null;

				fileSet = chord.retrieve(fileKey);

				for(Serializable oldMetadata : fileSet) {
					chord.remove(fileKey, oldMetadata);
					newMetadata = Metadata.createMetadata(oldMetadata.toString());
					//System.out.println("Inicio metadata do ficheiro: \n" + fileMetadata.getMetadata());
				}
				*/
				
				// TODO: trocar isto
				//trocar a funcao getSHA1 de sitio!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
				
				byte[] buffer = newBlock.toByteArray();
				
				String shaBuffer = new String(Client.bytesToHex(Client.getSHA1(buffer)));

				fileMetadata.updateBlock(blockNo, shaBuffer);

				int blockDiff = newBlock.size() - oldBlockLength;
				
				//System.out.println("oldBuffer: "  + new String(oldBuffer).length());
				//System.out.println("buffer: "  + new String(buffer).length());
				
				//System.out.println("blockDiff: " + blockDiff);

				fileMetadata.inc(blockDiff);

				//System.out.println("SHA: " + shaBuffer);
				

				//System.out.println("Nova metadata do ficheiro: \n" + newMetadata.getMetadata());

				//insere bloco
				MyKey key = new MyKey(shaBuffer);
				chord.insert(key, buffer);
				//chord.insert(key, new String(byteBuffer.array(), Charset.forName("UTF-8")));



				//System.out.println("Content do ficheiro: "  + new String(buffer));

			}
			
		
			chord.insert(fileKey, fileMetadata.getMetadata());

		} catch(Exception e) {
			System.out.println("ERRO-----" + e.getMessage());
		}

		
		
		
		//System.out.println("||||||||||||||||||||||||||||||||||||||||||||||||");

		return (int) bufSize;

	}	


	@Override
	public int create(final String path, final ModeWrapper mode, final FileInfoWrapper info) {


		System.out.println("function create (Fs.java)");

		System.out.println("-------------------------------__>Path passado para a funcao create: " + path);

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
				//remover primeiro chord.remove(Key, Serializable)

				newMetadata.addFile(pathObject.getFileName().toString());


				chord.insert(key, newMetadata.getMetadata());				

				key = new MyKey(path);

				Metadata content = Metadata.createMetadata(MetadataType.FILE.name() + "\n");
				chord.insert(key, content.getMetadata());


			} else {

				return -ErrorCodes.EEXIST(); //File already exists

			}

		} catch (ServiceException e) {
			e.printStackTrace();
		}

		return 0;

	}

	@Override
	public int rename(final String path, final String newName) {

		System.out.println("rename - Fs.java");

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

		System.out.println("---------------------- unlink (Fs.java)");

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
				//remover primeiro chord.remove(Key, Serializable)

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

		System.out.println("function mkdir (Fs.java)");

		System.out.println("PREDECESSOR: " + ((ChordImpl) chord).printPredecessor());
		System.out.println("------------------------------------------------");


		System.out.println("SUCCESSOR: " + ((ChordImpl) chord).printSuccessorList());
		System.out.println("------------------------------------------------");


		mode.setMode(NodeType.DIRECTORY, true, true, true, true, true, true, true, true, true);
		Set<Serializable> dataSet = null;

		Path pathObject = FileSystems.getDefault().getPath(path);
		System.out.println("Path: " + pathObject.toString());

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

		System.out.println("------------------------------BYTES------------------------------------");

		int bytes = 0;
		for (Map.Entry<ID, Set<Entry>> entry : ((ChordImpl) chord).getEntries().getEntries().entrySet()) {


			for (Entry setEntry : entry.getValue()) {
				System.out.print("|" + setEntry.getValue().toString() + "|");
				bytes += setEntry.getValue().toString().getBytes().length;
			}

		}

		System.out.println("BYTES: " + bytes);
		System.out.println("------------------------------------------------------------------");


		System.out.println("---------------------------SUCESSSORS AND PREDECESSOR---------------------------------");

		URL url = ((ChordImpl) chord).getReferences().getPredecessor().getNodeURL();
		System.out.println("URL(P): " + url.getHost() + ":" + url.getPort());


		for(Node node : ((ChordImpl) chord).getReferences().getSuccessors()) {

			System.out.println("URL(S)" + node.getNodeURL().getHost() + ":" + node.getNodeURL().getPort()); 

		}

		System.out.println("------------------------------------------------------------------");


		return 0;

	}

	@Override
	public int rmdir(final String path)
	{
		System.out.println("------------------------------function rmdir (Fs.java) in path: " + path);

		Set<Serializable> dataSet = null;

		Path pathObject = FileSystems.getDefault().getPath(path);

		System.out.println("pathObject: " + path);

		try {

			Metadata newMetadata = null;

			dataSet = chord.retrieve(new MyKey(path));


			if(!dataSet.isEmpty()) {

				int i = 0;
				for (Serializable serial : dataSet) {

					System.out.println("dataset" + i + " " + serial.toString());
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
				System.out.println(">>>>>>>" + pathObject.getParent().toString());

				Set<Serializable> set = chord.retrieve(key);

				for(Serializable tempSet : set) {
					chord.remove(key, tempSet);
					newMetadata = Metadata.createMetadata(tempSet.toString());
				}
				System.out.println(">>>>>>>" + pathObject.getFileName().toString());
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

	//	@Override
	//	public void afterUnmount(final File mountPoint) {
	//		mountPoint.delete();
	//	}



}
