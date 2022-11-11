package net.devtech.fastzipfilesystem;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.StandardOpenOption;
import java.nio.file.WatchService;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.regex.Pattern;

class FastZipFS extends FileSystem {
	private static final ByteBuffer ROOT_NAME = ByteBuffer.wrap(new byte[] {(byte) '/'});
	final FastZipFSProvider provider;
	final Path zipfsPath;
	final boolean readonly;
	final FastZipPath root = new FastZipPath(this, "/");
	final ConcurrentMap<ByteBuffer, FastZipEntry> paths;
	final NavigableSet<ByteBuffer> order; // todo linked list order instead of navigable set
	volatile boolean dirty; // todo use atomics
	boolean isOpen = true;
	
	FastZipFS(FastZipFSProvider provider, Path path, Map<String, ?> config) throws IOException {
		this.provider = provider;
		this.zipfsPath = path;
		this.readonly = "true".equals(config.get("readonly")) || !Files.isWritable(path);
		
		ConcurrentMap<ByteBuffer, FastZipEntry> paths = this.paths = new ConcurrentHashMap<>();
		NavigableSet<ByteBuffer> order = this.order = "true".equals(config.get("maintainOrder")) ? new ConcurrentSkipListSet<>() : null;
		FastZipEntry rootEntry = new FastZipEntry(this.root.name);
		BigByteBuffer.PathBuffer buf = BigByteBuffer.buffer(path);
		BigByteBuffer buffer = buf.buffer();
		FastZipReader.read(buffer, entry -> {
			ByteBuffer name = entry.name;
			paths.put(name, entry);
			if(order != null) {
				order.add(name);
			}
		}, () -> {
			paths.clear();
			if(order != null) {
				order.clear();
			}
			
			paths.put(ROOT_NAME, rootEntry);
			if(order != null) {
				order.add(ROOT_NAME);
			}
		});
		
		for(FastZipEntry value : paths.values()) {
			if(value != rootEntry) {
				FastZipEntry parent = this.getParent(value.name);
				parent.children.add(value.name);
			}
		}
	}
	
	FastZipEntry getOrCreatePath(ByteBuffer name, boolean directory) throws FileNotFoundException {
		name = absoluteName(name, directory);
		FastZipEntry parent = this.getParent(name);
		if(parent == null) {
			throw new FileNotFoundException(FastZipUtil.toStr(name) + "'s parent");
		}
		FastZipEntry entry = new FastZipEntry(name);
		FastZipEntry path = this.paths.putIfAbsent(name, entry);
		if(path == null) {
			if(this.order != null) {
				this.order.add(name);
			}
			parent.children.add(name);
		}
		return path == null ? entry : path;
	}
	
	static ByteBuffer absoluteName(ByteBuffer name, boolean directory) {
		int limit = name.limit();
		if((limit <= 0 || name.get(limit - 1) != '/')) {
			if(directory) {
				ByteBuffer buffer = ByteBuffer.allocate(limit + 1);
				buffer.put(0, name, 0, limit);
				buffer.put(limit, (byte) '/');
				name = buffer;
			}
		} else if(!directory) {
			throw new IllegalArgumentException(FastZipUtil.toStr(name) + " is a directory path!");
		}
		return name;
	}
	
	boolean removePath(FastZipPath paths) throws FileNotFoundException {
		FastZipEntry remove = this.paths.remove(paths.name);
		FastZipEntry parent = this.getParent(paths.name);
		if(remove != null) {
			paths.remove(remove);
		}
		
		if(remove != null) {
			if(this.order != null) {
				this.order.remove(paths.name);
			}
			
			parent.children.remove(paths.name);
		}
		
		return remove != null;
	}
	
	final FastZipEntry getParent(ByteBuffer name) throws FileNotFoundException {
		for(int i = name.limit() - 2; i >= 1; i--) {
			if(name.get(i) == '/') {
				ByteBuffer parentName = ByteBuffer.allocate(i + 1);
				parentName.put(0, name, 0, i + 1);
				return this.paths.get(parentName);
			}
		}
		return this.root.getEntry(true);
	}
	
	@Override
	public void close() throws IOException {
		if(this.isOpen) {
			this.flush();
			this.provider.filesystems.remove(this.root, this);
			this.isOpen = false;
		}
	}
	
	public void flush() throws IOException {
		if(this.dirty) {
			this.dirty = false;
			Path tempZip = Files.createTempFile("temp", ".zip");
			try(SeekableByteChannel channel = Files.newByteChannel(tempZip, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
				Collection<ByteBuffer> entries;
				if(this.order != null) {
					entries = this.order;
				} else {
					ArrayList<ByteBuffer> entryList = new ArrayList<>(this.paths.keySet());
					entryList.sort(Comparator.comparing(b -> {
						if(b.limit() == 0 || b.get(b.limit() - 1) == '/') {
							return b.limit();
						} else {
							return Integer.MAX_VALUE;
						}
					}));
					entries = entryList;
				}
				
				ByteBuffer temp = ByteBuffer.allocate(1024).order(ByteOrder.LITTLE_ENDIAN);
				
				// local file header
				long[] offsets = new long[entries.size()];
				int index = 0;
				for(ByteBuffer name : entries) {
					//if(name.limit() == 1 && name.get(0) == '/') {
					//	continue;
					//}
					offsets[index++] = channel.position();
					FastZipEntry entry = this.paths.get(name);
					FastZipEntry.ZipContents contents = entry.contents;
					if(contents == null) {
						contents = new FastZipEntry.ZipContents();
					}
					temp.putInt(FastZipReader.LOC_HEADER);
					temp.putShort((short) 0x14); // version
					temp.putShort((short) 0); // flag
					temp.putShort(contents.compressionMethod);
					temp.putInt((int) FastZipUtil.javaToDosTime(entry.lastMod));
					temp.putInt(contents.crc32);
					temp.putInt((int) contents.compressedSize); // todo zip64
					temp.putInt((int) contents.uncompressedSize);
					temp.putShort((short) name.limit());
					temp.putShort((short) 0); // todo zip64 (extra)
					temp.flip();
					channel.write(temp);
					temp.clear();
					channel.write(name);
					name.clear();
					if(entry.contents != null) {
						entry.contents.compress().segmentedInsert(buffer -> {
							try {
								channel.write(buffer);
							} catch(IOException e) {
								throw new RuntimeException(e);
							}
						}, contents.compressedOffset, contents.compressedSize);
					}
				}

				index = 0;
				long start = channel.position();
				for(ByteBuffer name : entries) { // cen
					//if(name.limit() == 1 && name.get(0) == '/') {
					//	continue;
					//}
					FastZipEntry entry = this.paths.get(name);
					FastZipEntry.ZipContents contents = entry.contents;
					if(contents == null) {
						contents = new FastZipEntry.ZipContents();
					}
					temp.putInt(FastZipReader.CEN_HEADER);
					temp.putShort((short) 0x31e);
					temp.putShort((short) 20);
					temp.putShort((short) 0);
					temp.putShort(contents.compressionMethod);
					temp.putInt((int) FastZipUtil.javaToDosTime(entry.lastMod));
					temp.putInt(contents.crc32);
					temp.putInt((int) contents.compressedSize); // todo zip64
					temp.putInt((int) contents.uncompressedSize);
					temp.putShort((short) name.limit());
					temp.putShort((short) 0); // todo zip64 extra
					temp.putShort((short) 0); // comment len
					temp.putShort((short) 0); // disk
					temp.putShort((short) 0); // internal file attributes
					temp.putInt((short) 0); // external file attributes
					temp.putInt((int) offsets[index++]);
					temp.flip();
					channel.write(temp);
					temp.clear();
					channel.write(name);
				}
				long end = channel.position();
				
				// eocd
				temp.clear();
				temp.putInt(FastZipReader.EOCD_HEADER);
				temp.putShort((short) 0);
				temp.putShort((short) 0);
				temp.putShort((short) entries.size());
				temp.putShort((short) entries.size());
				temp.putInt((int) (end - start));
				temp.putInt((int) start);
				temp.putShort((short) 0);
				temp.flip();
				channel.write(temp);
				temp.clear();
			}
			try {
				Files.deleteIfExists(this.zipfsPath);
				Files.copy(tempZip, this.zipfsPath);
			} finally {
				Files.deleteIfExists(tempZip);
			}
			
		}
	}
	
	@Override
	public Iterable<Path> getRootDirectories() {
		return List.of(this.root);
	}
	
	@Override
	public Iterable<FileStore> getFileStores() {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public Set<String> supportedFileAttributeViews() {
		return Set.of("basic");
	}
	
	@Override
	public Path getPath(String first, String... more) {
		if(more == null || more.length == 0) {
			return new FastZipPath(this, first);
		}
		StringBuilder sb = new StringBuilder();
		sb.append(first);
		for(String path : more) {
			if(path.length() > 0) {
				if(sb.length() > 0) {
					sb.append('/');
				}
				sb.append(path);
			}
		}
		return new FastZipPath(this, sb.toString());
	}
	
	static final String GLOB_SYNTAX = "glob";
	static final String REGEX_SYNTAX = "regex";
	
	@Override
	public PathMatcher getPathMatcher(String syntaxAndInput) {
		int pos = syntaxAndInput.indexOf(':');
		if(pos <= 0) {
			throw new IllegalArgumentException();
		}
		String syntax = syntaxAndInput.substring(0, pos);
		String input = syntaxAndInput.substring(pos + 1);
		String expr;
		if(syntax.equalsIgnoreCase(GLOB_SYNTAX)) {
			expr = FastZipUtil.toRegexPattern(input);
		} else {
			if(syntax.equalsIgnoreCase(REGEX_SYNTAX)) {
				expr = input;
			} else {
				throw new UnsupportedOperationException("Syntax '" + syntax + "' not recognized");
			}
		}
		// return matcher
		final Pattern pattern = Pattern.compile(expr);
		return (path) -> pattern.matcher(path.toString()).matches();
	}
	
	@Override
	public UserPrincipalLookupService getUserPrincipalLookupService() {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public WatchService newWatchService() throws IOException {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public boolean isOpen() {
		return this.isOpen;
	}
	
	@Override
	public boolean isReadOnly() {
		return this.readonly;
	}
	
	@Override
	public String getSeparator() {
		return "/";
	}
	
	@Override
	public FileSystemProvider provider() {
		return this.provider;
	}
	
}
