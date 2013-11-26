package fuse;

import java.io.File;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Set;

import chord.MyKey;
import de.uniba.wiai.lspi.chord.service.Chord;
import de.uniba.wiai.lspi.chord.service.ServiceException;
import de.uniba.wiai.lspi.chord.service.impl.ChordImpl;
import net.fusejna.DirectoryFiller;
import net.fusejna.ErrorCodes;
import net.fusejna.FuseException;
import net.fusejna.StructFuseFileInfo.FileInfoWrapper;
import net.fusejna.StructStat.StatWrapper;
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

		System.out.println("<initializeFuse>");

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

		System.out.println("<getattr>");

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






}
