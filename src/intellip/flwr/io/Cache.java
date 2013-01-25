package intellip.flwr.io.cache;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.UUID;

public abstract class Cache {
	
	/*
	 * Cache file properties
	 */
	private String cachePath;
	private String cacheType;
	private String cacheName;
	
	/*
	 * memory-map and byte buffer related properties
	 */
	protected RandomAccessFile dataCacheFile;
	protected final long blockSize;
	
	/* CONSTRUCTOR
	 * ------------------------------------------------------------------
	 * Constructor is used to set the cache path, block size and name
	 * path is the directory location where cache file is stored
	 * block size denotes the size of one partition or map in the file.
	 * type is the type of cache (default is bin.cac). This is the 
	 * extension of cache file name.
	 * If name is not supplied, cache file name is generated randomly
	 * and concatenated with the type.
	 */
	public Cache(String path) throws Exception  {
		cachePath = path;
		if(!isValidPath(cachePath)) throw new FileNotFoundException(path);
		cacheType = "bin.cac";
		cacheName = String.valueOf(Math.abs(UUID.randomUUID().getMostSignificantBits())) + "." + cacheType;
		dataCacheFile = new RandomAccessFile(cachePath + cacheName, "rw");
		blockSize = 1 << 13; // 64MB default
	}
	public Cache(String path, long block_size) throws Exception  {
		cachePath = path;
		if(!isValidPath(cachePath)) throw new FileNotFoundException(path);
		cacheType = "bin.cac";
		cacheName = String.valueOf(Math.abs(UUID.randomUUID().getMostSignificantBits())) + "." + cacheType;
		dataCacheFile = new RandomAccessFile(cachePath + cacheName, "rw");
		blockSize = block_size;
	}
	public Cache (String path, long block_size, String name) throws Exception {
		cachePath = path;
		if(!isValidPath(cachePath)) throw new FileNotFoundException(path);
		cacheType = "bin.cac";
		cacheName = name + "." + cacheType;
		dataCacheFile = new RandomAccessFile(cachePath + cacheName, "rw");
		blockSize = block_size;
	}
	public Cache (String path, long block_size, String name, String type) throws Exception {
		cachePath = path;
		if(!isValidPath(cachePath)) throw new FileNotFoundException(path);
		cacheType = type;
		cacheName = name + "." + type;
		dataCacheFile = new RandomAccessFile(cachePath + cacheName, "rw");
		blockSize = block_size;
	}
	
	/*
	 * ABSTRACT METHODS
	 * These are the abstract methods required to be implemented
	 * in the classes inherited from this
	 * ------------------------------------------------------------------
	 */

	public abstract long set(byte[] bytes) throws IOException;
	public abstract byte[] get(long pos);
	
	/* 
	 * ACCESSORS AND MUTATORS
	 * Various SETTER and GETTER methods And other trivial 
	 * methods that are implemented fully in the abstract class
	 * ------------------------------------------------------------------
	 */
	// returns the cache name (cache file name minus extension)
	public String getCacheName() {
		return cacheName.substring(1, 16);
	}	 
	// returns the cache file name
	public String getCacheFileName() {
		return cacheName;
	}
	// returns the cache file path
	public String getCacheFilePath() {
		return cachePath + cacheName;
	}
	// returns the cache file size, i.e. the amount of disk space occupied
	public long getCacheSizeDisk () throws IOException {
		return dataCacheFile.length();
	}
	// get the default size of one block or partition or map
	public long getBlockSize() {
		return blockSize;
	}
	
	
	/*
	 * Helper methods 
	 * ------------------------------------------------------------------
	 */
	protected boolean isValidPath(String path) {
		File f = new File(path);
		if (!f.exists()) 
			return false;
		else return true;
	}
	
}
