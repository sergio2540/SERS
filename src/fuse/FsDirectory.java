package fuse;

import net.fusejna.types.TypeMode.ModeWrapper;

public class FsDirectory extends Fs {

	@Override
	 public int mkdir(final String path, final ModeWrapper mode)
   {
		
		for(int i = 0; i < 1000; i++) {}
		
		System.out.println("function mkdir (FsDirectory.java)");
		
		for(int i = 0; i < 1000; i++) {}
		
		
		return 0;
			
		 
   }
	
	
}
