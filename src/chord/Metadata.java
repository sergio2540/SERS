package chord;

import java.util.ArrayList;
import java.util.Collection;

public class Metadata {

	private MetadataType metadataType;
	private Collection<String> files;
	private int size;
	private Collection<String> blockPaths;
	

	// TODO: BLOCKSIZE REPETIDO MUDAR ISTO
	private final int BLOCKSIZE = 1024; 

	public Metadata(MetadataType type, Collection<String> files, int size, Collection<String> blockPaths){
		this.metadataType = type;
		this.files = files;
		this.size = size;
		this.blockPaths = blockPaths;
	}

	public MetadataType getMetadataType() {
		return this.metadataType;
	}

	public Collection<String> getFiles() {
		return this.files;
	}

	public int getSize() {
		return this.size;
	}

	public Collection<String> getBlocksPaths() {
		return this.blockPaths;
	}

	public static Metadata createMetadata(String metadata) {

		String [] splittedMetadata = metadata.split("\n");
		Collection<String> files;
		Collection<String> blocks;

		int size = 0;

		if(splittedMetadata[0].equals("DIR")) {

			files = new ArrayList<String>();

			for (int i = 1; i < splittedMetadata.length; i++) {

				files.add(splittedMetadata[i]);

			}

			return new Metadata(MetadataType.DIR, files, size, null);

		} else if(splittedMetadata[0].equals("FILE")) {

			blocks = new ArrayList<String>();

			if(splittedMetadata.length > 1)
				size = Integer.parseInt(splittedMetadata[1]);

			for (int i = 2; i < splittedMetadata.length; i++) {

				blocks.add(splittedMetadata[i]);

			}

			return new Metadata(MetadataType.FILE, null, size, blocks);

		} else {

			return null;

		}

	}

	public String getMetadata() {

		StringBuilder metadata = new StringBuilder();

		metadata.append(this.getMetadataType().name());
		if(this.getMetadataType() == MetadataType.DIR) {
			metadata.append("\n");
			for(String file : this.getFiles()) {
				metadata.append(file);
				metadata.append("\n");
			}

		} else if(this.getMetadataType() == MetadataType.FILE) {
			metadata.append("\n");
			metadata.append(this.getSize());
			metadata.append("\n");
			for(String block : this.getBlocksPaths()) {
				metadata.append(block);
				metadata.append("\n");
			}

		} else {

			return null;

		}

		return metadata.toString();

	}

	public void addDir(String newDir) {
		this.files.add(newDir);
	}

	public void rmDir(String dirToRemove) {
		this.files.remove(dirToRemove);	
	}

	public void addFile(String fileToAdd) {
		this.files.add(fileToAdd);
	}
	
	public void removeFile(String fileToRemove) {
		this.files.remove(fileToRemove);
	}
	
	public void addBlock(String hash) {
		
		this.blockPaths.add(hash);	
	
	}
	
	public void updateBlock(int index, String hash) {
		
		ArrayList<String> temp = (ArrayList<String>) this.blockPaths;
		
		if(index < this.blockPaths.size()) {
			temp.set(index, hash);
			
		} else {
			addBlock(hash);
		}
		
		
		
	}
	
	public void removeBlock(String hash) {
		this.blockPaths.remove(hash);	
	}
	
	public void inc(int size) {
		this.size += size;		
	}
	
}


