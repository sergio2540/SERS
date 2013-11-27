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
import net.fusejna.util.FuseFilesystemAdapterFull;



public class Fs extends FuseFilesystemAdapterFull {

	private static Fs fs;
	private Chord chord;
	private String path;
	private static String content = "";

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

		System.out.println("function getattr (Fs.java)");

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

		return -ErrorCodes.ENOENT();

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

					}

				}
			} catch (ServiceException e) {
				e.printStackTrace();
			}
		}

		// Compute substring that we are being asked to read
		s = content.substring((int) offset, (int) Math.max(offset, Math.min(content.length() - offset, offset + size)));
		buffer.put(s.getBytes());

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
			metaInfo = data.toString().split("\n");
			fileOrDir = metaInfo[0];
		}

		if(!fileOrDir.equals("DIR")) {
			System.out.println("!fileOrDir.equals(DIR) - Fs.java");
			return -1;
		}

		for(int i = 1; i < metaInfo.length; i++) {
			filler.add(metaInfo[i]);
		}

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
	
	/*
	@Override
    public int write(final String path, final ByteBuffer buf, final long bufSize, final long writeOffset, final FileInfoWrapper wrapper)
    {
		
			MyKey key = new MyKey(path);
			Set<Serializable> metaDataSet = chord.retrieve(key);
			Metadata metaData = null;
			for(Serializable ser : metaDataSet){
				metaData = Metadata.createMetadata(ser.toString());  
			}
			
			if(metaDataSet.isEmpty())
				return -ErrorCodes.ENOENT();
			
			if (metaData.getMetadataType() == MetadataType.DIR) 
				 return -ErrorCodes.EISDIR();
			
			Collection<String> blockPaths = metaData.getBlocksPaths();
			Set<Serializable> partialData = null;
			for(String block : blockPaths){
				partialData  = chord.retrieve(new MyKey(block));
				
			}


    		
            return ((MemoryFile) p).write(buf, bufSize, writeOffset);
    }*/

	@Override
	public int mkdir(final String path, final ModeWrapper mode)
	{

		System.out.println("function mkdir (Fs.java)");

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
					if(newMetadata.getFiles().size() != 0)
						return -ErrorCodes.ENOENT();//ver melhor
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

}
