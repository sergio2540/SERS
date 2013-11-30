package fuse;

import java.io.File;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import chord.Metadata;
import chord.MetadataType;
import chord.MyKey;
import de.uniba.wiai.lspi.chord.data.ID;
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
import net.fusejna.util.FuseFilesystemAdapterFull;
import chord.Client;



public class Fs extends FuseFilesystemAdapterAssumeImplemented {

	private static Fs fs;
	private Chord chord;
	private String path;
	private static String content = "";

	private final int BLOCKSIZE = 1024;
	private byte buffer[] = new byte[BLOCKSIZE];
	private int cursorByte = 0;

	public Fs() {}

	private Fs(Chord chord, String path) {
		this.chord = chord;
		this.path = path;
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

		System.out.println("function getattr (Fs.java) PATH: " + path);

		String metaInfo[] = null;
		String fileOrDir = "";
		int size = 0;

		File file = new File(path);

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
			System.out.println("FILE IS DIRECTORY!");
			stat.setMode(NodeType.DIRECTORY);
			return 0;
		}

		if(fileOrDir.equals("FILE")) {
			size = Integer.parseInt(metaInfo[1]);
			System.out.println("FILE IS FILE!");
			stat.setMode(NodeType.FILE).size(size);
			return 0;
		}

		System.out.println("not a file or dir");

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

		System.out.println("function read (Fs.java)");

		System.out.println("----read start---");
		System.out.println("path: " + path);
		System.out.println("----read---");
		System.out.println("<read>");

		String s = "";
		String metaInfo[] = null;
		String fileOrDir = "";

		if(offset == 0) {

			try {

				Collection<Serializable> list = new ArrayList<Serializable>();
				
				Set<Serializable> dataSet = null;
				dataSet = chord.retrieve(new MyKey(path));

				if(dataSet.isEmpty()) {

					System.out.println("data empty");
					return -1;

				} else {

					for(Serializable data : dataSet) {
						metaInfo = data.toString().split("\n");
						fileOrDir = metaInfo[0];
					}

					//dataSet.clear();

					if(fileOrDir.equals("DIR")) {
						System.out.println("function read - Fs.java");
						return -1;
					} else if(fileOrDir.equals("FILE")) {

						for(int i = 2; i < metaInfo.length; i++) {
							list.addAll(chord.retrieve(new MyKey(metaInfo[i])));

						}

						for(Serializable data : list) {
							content += data.toString();
						}

						System.out.println("CONTENT ON READ: " + content);

					}

				}
			} catch (ServiceException e) {
				e.printStackTrace();
			}
		}

		System.out.println("entrou");
		// Compute substring that we are being asked to read
		s = content.substring((int) offset, (int) Math.max(offset, Math.min(content.length() - offset, offset + size)));
		buffer.put(s.getBytes());

		System.out.println("saiu bytes: " + s);

		return s.getBytes().length;

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
			return -1;
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

	/*
	private int write(final ByteBuffer buffer, final long bufSize, final long writeOffset)
    {
            final int maxWriteIndex = (int) (writeOffset + bufSize);
            if (maxWriteIndex > contents.capacity()) {
                    // Need to create a new, larger buffer
                    final ByteBuffer newContents = ByteBuffer.allocate(maxWriteIndex);
                    newContents.put(contents);
                    contents = newContents;
            }
            final byte[] bytesToWrite = new byte[(int) bufSize];
            buffer.get(bytesToWrite, 0, (int) bufSize);
            contents.position((int) writeOffset);
            contents.put(bytesToWrite);
            contents.position(0); // Rewind
            return (int) bufSize;
    }*/


	//	@Override
	//    public int write(final String path, final ByteBuffer buf, final long bufSize, final long writeOffset, final FileInfoWrapper wrapper)
	//    {
	//		//DEBUG
	//		
	//		
	//		
	//		System.out.println("Path: " + path);
	//		System.out.println("bufSize: " + bufSize);
	//		System.out.println("writeOffset: " + writeOffset);
	//
	//		//cursorByte = (int) writeOffset;
	//		
	//		int bufferSize = (int) bufSize;
	//		
	//		if(buffer.length + bufSize >= BLOCKSIZE && writeOffset == cursorByte) {
	//			
	//			int bufCursor = 0;
	//			
	//			while(buffer.length < BLOCKSIZE) {
	//				
	//				buffer[cursorByte] = buf.get(bufCursor);
	//				bufCursor++;
	//				cursorByte++;
	//				bufferSize--;			
	//				
	//			}
	//			
	//			//escrever
	//			//clear ao buffer
	//			cursorByte = 0;
	//			
	//		} else {
	//			
	//		}
	//		
	//		for(long i = 0; i < bufSize; i++) {
	//			buffer[(int) i] = buf.get((int) i);
	//		}
	//			
	//		cursorByte += bufSize;
	//		
	//		if(cursorByte > buffer.length - 1) {
	//			System.out.println("Buffer cheio. Enviar para a DHT");
	//			
	//		}
	//		
	//		return 0;
	//    }

	@Override
	public int write(final String path, final ByteBuffer buf, final long bufSize, final long writeOffset, final FileInfoWrapper wrapper) {
		
		//bufsize = 1048
		//writeoffset= 2100
		
		System.out.println("bufSize: " + bufSize + "writeOffSet: " + writeOffset);
		
		//2100/1024 = indice 2 bloco
		int beginOffSet = (int) writeOffset/BLOCKSIZE;
		
		// (1048 + 2100) /1024 = indice 3 bloco
		int endOffSet = (int ) ((bufSize + writeOffset) / BLOCKSIZE);
		
		
		//2100%1024 = 52 bytes correctos
		int cursorStartByte = (int) writeOffset % BLOCKSIZE;
		
		//(2100 + 1048) % 1024 = 76 
		//(2100 + 1048) = 3148 escrever ate 3148
		int cursorEndByte = (int) (bufSize + writeOffset);
		
		
		//path = hello.txt
		MyKey fileKey = new MyKey(path);
		
		Set<Serializable> dataSet = null;
		 
		try {
		
			dataSet = chord.retrieve(fileKey);
		
			Metadata fileMetadata = null;
		
		Path pathObject = FileSystems.getDefault().getPath(path);
		
		for(Serializable metadata : dataSet){
			
			fileMetadata = Metadata.createMetadata(metadata.toString());
		}

		ArrayList<String> blocks = (ArrayList<String>) fileMetadata.getBlocksPaths();//-------------------sapateiro

		int bufIndex = 0;
		
		//2 .. 3
		for(int blockNo = beginOffSet; blockNo <= endOffSet; blockNo++){
		
			System.out.println("Block nº: " + blockNo);
		
			String content = null;
			
			System.out.println("\n----antes-----block\n");
			String block = blocks.get(blockNo);
			System.out.println("\n----depois-----block\n");
			
			System.out.println("\n-----------------------------------retrieve--antes----------\n");
			Set<Serializable> data = chord.retrieve(new MyKey(block));
			System.out.println("\n--------------------------------retrieve--depois-----------------\n");
			
			if(!data.isEmpty()) {
				System.out.println("data is not empty");
			
				for(Serializable ser : data) {
					content += ser.toString();
				}
				
				byte oldBuffer[] = content.getBytes();
				
				for(int index = 0 ;index < BLOCKSIZE; index++){
					buffer[index] = oldBuffer[index];
				}
				
			}
			
			System.out.println("cursor byte: " + cursorStartByte);
			System.out.println("bufIndex: " + bufIndex);
			System.out.println("bufsize: " + bufSize);
			
			for (int index = cursorStartByte, index2 = bufIndex; index2 < bufSize; index++, index2++) {
				
				System.out.println("cursor byte: " + cursorStartByte);
				System.out.println("bufsize: " + bufSize);
				
				if(index == BLOCKSIZE){			
					System.out.println("index == BLOCKSIZE");
					cursorStartByte = 0;
					bufIndex = index2;
					break;
				}
				
				
				buffer[index] = buf.get(index2);
			}
			
			//Hash
			//escrever para a dht
			System.out.println("Bloco adicionado à dht (chegou ao fim de buffer)");

		

			MyKey key = new MyKey(pathObject.toString());

			Set<Serializable> set = chord.retrieve(key);
			Metadata newMetadata = null;
			
			//update nova metadata
			for(Serializable oldMetadata : set) {
				chord.remove(key, oldMetadata);
				newMetadata = Metadata.createMetadata(oldMetadata.toString());
			}
			

			//trocar a funcao getSHA1 de sitio
			String shaBuffer = new String(Client.getSHA1(new String(buffer)));
			
			newMetadata.inc(new String(buffer).trim().length());
			
			newMetadata.updateBlock(blockNo, shaBuffer);
			
			System.out.println("SHA: " + shaBuffer);
			chord.insert(key, newMetadata.getMetadata());
			
			

			System.out.println("metadata do ficheiro a editar: \n" + newMetadata.getMetadata());

			
			//insere bloco
			key = new MyKey(shaBuffer);
			chord.insert(key, new String(buffer));

			System.out.println("Content do ficheiro: "  + new String(buffer));
			
			
			}

		} catch(Exception e) {
			System.out.println("ERRO-----" + e.getMessage());
		}

		return (int) bufSize;

	}	


	@Override
	public int create(final String path, final ModeWrapper mode, final FileInfoWrapper info) {


		System.out.println("function create (Fs.java)");

		System.out.println("Path passado para a funcao create: " + path);

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

				return -1; //File already exists

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

				return -1; //File already exists
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
				//remover primeiro chord.remove(Key, Serializable)

				newMetadata.addDir(pathObject.getFileName().toString());

				chord.insert(key, newMetadata.getMetadata());				

				key = new MyKey(path);

				Metadata content = Metadata.createMetadata(MetadataType.DIR.name());
				chord.insert(key, content.getMetadata());


			} else {

				return -1; //verficar o código de ERROS do FUSE

			}

		} catch (ServiceException e) {
			e.printStackTrace();
		}

		return 0;

	}

	@Override
	public int rmdir(final String path)
	{
		System.out.println("function rmdir (Fs.java) in path: " + path);

		Set<Serializable> dataSet = null;

		Path pathObject = FileSystems.getDefault().getPath(path);

		System.out.println("pathObject: " + path);
		//System.out.println("Path: " + pathObject.toString());

		try {

			Metadata newMetadata = null;

			dataSet = chord.retrieve(new MyKey(path));


			if(!dataSet.isEmpty()) {//se criarmos e removermos uma hierarquia, se criarmos de novo a pasta a hierarquia ainda existe

				int i = 0;
				for (Serializable serial : dataSet) {

					System.out.println("dataset" + i + " " + serial.toString());
					i++;

					newMetadata = Metadata.createMetadata(serial.toString());
					if(newMetadata.getFiles().size() != 0) {
						//						return -ErrorCodes.ENOENT();//ver melhor
						return 0;
					}
				}

				for(Serializable oldMetadata : dataSet) {
					chord.remove(new MyKey(path), oldMetadata);	
				}
				//remover primeiro chord.remove(Key, Serializable)

				//newMetadata.addDir(pathObject.getFileName().toString());

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

				return -1; //verficar o código de ERROS do FUSE

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

}
