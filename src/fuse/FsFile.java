package fuse;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Set;

import net.fusejna.StructFuseFileInfo.FileInfoWrapper;
import chord.MyKey;
import de.uniba.wiai.lspi.chord.service.Chord;
import de.uniba.wiai.lspi.chord.service.ServiceException;

public class FsFile extends Fs {

	private Chord chord;
	private String pathDir;
	private String name;
	private String content;

	public FsFile(Chord chord, String pathDir, String name, String content) {
		this.chord = chord;
		this.pathDir = pathDir;
		this.name = name;
		this.content = content;
		
		createFile();
		//DHT
		//UPDATE DIR 
		//CRIAR METADADOS
		//CRIAR BLOCOS
	}



	private void createFile() {

		

	}


	private void updateDHT(String pathDir) {

		Set<Serializable> dataSet = null;
		String[] metaInfo;
		String fileOrDir;
		
		try {
			dataSet = chord.retrieve(new MyKey(pathDir));
		} catch (ServiceException e) {
			e.printStackTrace();
		}

		for(Serializable data : dataSet) {
			metaInfo = data.toString().split("\n");
			fileOrDir = metaInfo[0];
		}


	}
	
	
	
	

}
