package fuse;

import java.io.Serializable;
import java.util.Set;

import chord.MyKey;
import de.uniba.wiai.lspi.chord.service.ServiceException;

public class FsFile extends Fs {

	private String pathDir;
	private String name;
	private String content;

	public FsFile(String pathDir, String name, String content) {
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


	private 



}
