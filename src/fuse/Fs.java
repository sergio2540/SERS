package fuse;

import java.io.File;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Iterator;
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

					dataSet.clear();

					if(fileOrDir.equals("DIR")) {
						System.out.println("function read - Fs.java");
						return -1;
					} else if(fileOrDir.equals("FILE")) {

						for(int i = 2; i < metaInfo.length; i++) {
							dataSet.addAll(chord.retrieve(new MyKey(metaInfo[i])));
							
						}

						for(Serializable data : dataSet) {
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
		
		
		Set<Serializable> dataSet = null;

		Path pathObject = FileSystems.getDefault().getPath(path);
		

		try {

			dataSet = chord.retrieve(new MyKey(path));

			if(!dataSet.isEmpty()) { 
				System.out.println("write dataset is not empty");
			}

			if(dataSet.isEmpty()) {
				System.out.println("write dataset is empty");
			}
			
			if(!dataSet.isEmpty()) {

				MyKey key = new MyKey(pathObject.toString());

				Set<Serializable> set = chord.retrieve(key);
				Metadata newMetadata = null;

				for(Serializable oldMetadata : set) {
					chord.remove(key, oldMetadata);
					newMetadata = Metadata.createMetadata(oldMetadata.toString());
				}
				
				int i = 0;
				while(i < bufSize) {
					buffer[i] = buf.get(i);
					i++;
				}
				
				//trocar a funcao getSHA1 de sitio
				String shaBuffer = new String(Client.getSHA1(new String(buffer)));
				newMetadata.addBlock(shaBuffer);
				System.out.println("SHA: " + shaBuffer);
				chord.insert(key, newMetadata.getMetadata());
				
				System.out.println("metadata do ficheiro a editar: \n" + newMetadata.getMetadata());
				
				key = new MyKey(shaBuffer);
				chord.insert(key, new String(buffer));
				
				System.out.println("Content do ficheiro: "  + new String(buffer));

			} else {

				return -1; //File already exists

			}
			
		} catch(Exception e) {
			
		}
		
		return 0;

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
